# Databricks notebook source
# MAGIC %md
# MAGIC # Transformer Attribution - NO FUNNEL EVENTS (No Data Leakage)
# MAGIC 
# MAGIC **Prevents data leakage by excluding funnel events that reveal conversion**
# MAGIC 
# MAGIC Excluded events:
# MAGIC - add_item, quick_add_item
# MAGIC - cart_page_load, checkout_page_load
# MAGIC - action_place_order, system_checkout_success
# MAGIC 
# MAGIC Included events:
# MAGIC - Impressions (m_card_view)
# MAGIC - Clicks (m_card_click)
# MAGIC - Store page views (store_page_load)
# MAGIC - Search, tab switches, etc.
# MAGIC 
# MAGIC Uses PyTorch Transformer with standard nn.TransformerEncoder

# COMMAND ----------

import os
import sys
import math
import random
from typing import List, Dict

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

from pyspark.sql import functions as F, Window

import torch
import torch.nn as nn
import torch.nn.functional as F_torch
from torch.utils.data import Dataset, DataLoader

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

import matplotlib.pyplot as plt
import seaborn as sns

# Random seeds
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snowflake Connection

# COMMAND ----------

your_scope = "fionafan-scope"
user = dbutils.secrets.get(scope=your_scope, key="snowflake-user")
password = dbutils.secrets.get(scope=your_scope, key="snowflake-password")

OPTIONS = dict(
    sfurl="doordash.snowflakecomputing.com/",
    sfuser=user, sfpassword=password, sfaccount="DOORDASH",
    sfdatabase="PRODDB", sfrole="fionafan",
    sfwarehouse="TEAM_DATA_ANALYTICS_ETL",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data - NO FUNNEL EVENTS

# COMMAND ----------

print("Loading events (EXCLUDING funnel events)...")

query = """
WITH sampled_users AS (
    SELECT DISTINCT USER_ID
    FROM proddb.fionafan.all_user_sessions_with_events
    WHERE USER_ID IS NOT NULL
    ORDER BY RANDOM()
    LIMIT 10000
),
session_conversions AS (
    -- Create conversion labels using funnel events
    SELECT DISTINCT
        e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM,
        MAX(CASE 
            WHEN e.EVENT_TYPE = 'funnel' 
            AND LOWER(e.EVENT) IN ('action_place_order', 'system_checkout_success') 
            THEN 1 ELSE 0 
        END) AS converted
    FROM proddb.fionafan.all_user_sessions_with_events e
    INNER JOIN sampled_users su ON e.USER_ID = su.USER_ID
    GROUP BY e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM
)
SELECT
    e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM, e.TIMESTAMP AS event_ts,
    COALESCE(e.EVENT_TYPE, 'UNKNOWN') AS event_type_norm,
    COALESCE(e.EVENT, 'UNKNOWN') AS event_norm,
    COALESCE(e.DISCOVERY_FEATURE, 'UNKNOWN') AS discovery_feature_norm,
    COALESCE(CAST(e.STORE_ID AS STRING), 'UNKNOWN') AS store_id,
    COALESCE(e.EVENT_RANK, 0) AS event_rank,
    sc.converted,
    CASE WHEN ds.NV_ORG IS NOT NULL OR ds.NV_VERTICAL_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_nv_store,
    COALESCE(ds.NAME, 'NO_STORE') AS store_name,
    COALESCE(ds.PRIMARY_TAG_NAME, 'NO_STORE') AS primary_tag_name,
    COALESCE(ds.PRIMARY_CATEGORY_NAME, 'NO_STORE') AS primary_category_name
FROM proddb.fionafan.all_user_sessions_with_events e
INNER JOIN session_conversions sc
    ON e.USER_ID = sc.USER_ID AND e.DD_DEVICE_ID = sc.DD_DEVICE_ID
    AND e.EVENT_DATE = sc.EVENT_DATE AND e.SESSION_NUM = sc.SESSION_NUM
LEFT JOIN edw.merchant.dimension_store ds ON e.STORE_ID = ds.STORE_ID
WHERE e.EVENT_TYPE NOT ILIKE '%error%' AND e.EVENT NOT ILIKE '%error%'
  -- EXCLUDE ALL FUNNEL EVENTS from sequence (but we keep conversion label)
  AND e.EVENT_TYPE != 'funnel'
"""

df_events = spark.read.format("snowflake").options(**OPTIONS).option("query", query).load()

# Add sequence number and time delta
window_spec = Window.partitionBy("USER_ID", "DD_DEVICE_ID", "EVENT_DATE", "SESSION_NUM").orderBy("event_ts")
df_events = df_events.withColumn("event_sequence_num", F.row_number().over(window_spec))
df_events = df_events.withColumn("seconds_from_prev", 
    F.coalesce(F.unix_timestamp("event_ts") - F.lag(F.unix_timestamp("event_ts")).over(window_spec), F.lit(0)))

df_events.cache()

event_count = df_events.count()
print(f"✓ Loaded {event_count:,} events (funnel events excluded)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Group into Sessions

# COMMAND ----------

print("Grouping into sessions...")

df_sessions = df_events.groupBy("USER_ID", "DD_DEVICE_ID", "EVENT_DATE", "SESSION_NUM").agg(
    F.collect_list(F.struct("event_norm", "discovery_feature_norm", "store_id", "store_name",
                            "primary_tag_name", "primary_category_name", "event_rank", 
                            "seconds_from_prev", "is_nv_store")).alias("events_array"),
    F.max("converted").alias("label"),
    F.count("*").alias("num_events")
)

df_sessions = df_sessions.filter(F.col("num_events") >= 2)
df_sessions = df_sessions.withColumn("session_id", F.concat_ws("_", "USER_ID", "DD_DEVICE_ID", "EVENT_DATE", "SESSION_NUM"))
df_sessions.cache()

session_count = df_sessions.count()
conversion_rate = df_sessions.agg(F.avg("label")).collect()[0][0]
print(f"✓ {session_count:,} sessions | Conversion: {conversion_rate:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert to Pandas & Build Vocabularies

# COMMAND ----------

MAX_SESSIONS = 30000
if session_count > MAX_SESSIONS:
    pdf_sessions = df_sessions.sample(fraction=MAX_SESSIONS/session_count, seed=SEED).select("session_id", "events_array", "label").toPandas()
else:
    pdf_sessions = df_sessions.select("session_id", "events_array", "label").toPandas()

print(f"✓ Converted {len(pdf_sessions):,} sessions to Pandas")

# COMMAND ----------

class Vocab:
    def __init__(self, min_freq=2, unk_token="<UNK>"):
        self.min_freq, self.unk_token, self.pad_token = min_freq, unk_token, "<PAD>"
        self.freqs, self.token_to_idx, self.idx_to_token = {}, {self.pad_token: 0, self.unk_token: 1}, [self.pad_token, self.unk_token]
    def add(self, token):
        self.freqs[token] = self.freqs.get(token, 0) + 1
    def build(self):
        for token, freq in sorted(self.freqs.items(), key=lambda x: (-x[1], x[0])):
            if token not in (self.pad_token, self.unk_token) and freq >= self.min_freq:
                self.token_to_idx[token] = len(self.idx_to_token)
                self.idx_to_token.append(token)
        print(f"  Built vocab: {len(self.idx_to_token)} tokens")
    def __len__(self):
        return len(self.idx_to_token)
    def encode(self, token):
        return self.token_to_idx.get(token, self.token_to_idx[self.unk_token])

print("Building vocabularies...")
vocabs = {}
for name, key in [("event", "event_norm"), ("feature", "discovery_feature_norm"), ("store", "store_id"),
                  ("store_name", "store_name"), ("store_tag", "primary_tag_name"), ("store_category", "primary_category_name")]:
    vocab = Vocab(min_freq=3)
    for _, row in pdf_sessions.iterrows():
        for event in row['events_array']:
            vocab.add(event[key] if name != "store" else event[key])
    vocab.build()
    vocabs[name] = vocab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Definition

# COMMAND ----------

class SessionDataset(Dataset):
    def __init__(self, sessions_df, vocabs, max_seq_len=100):
        self.sessions = sessions_df.to_dict('records')
        self.vocabs = vocabs
        self.max_seq_len = max_seq_len
    
    def __len__(self):
        return len(self.sessions)
    
    def __getitem__(self, idx):
        s = self.sessions[idx]
        events = s["events_array"][:self.max_seq_len]
        seq_len = len(events)
        
        event_tokens = np.array([self.vocabs["event"].encode(e['event_norm']) for e in events], dtype=np.int64)
        feature_tokens = np.array([self.vocabs["feature"].encode(e['discovery_feature_norm']) for e in events], dtype=np.int64)
        store_tokens = np.array([self.vocabs["store"].encode(e['store_id']) for e in events], dtype=np.int64)
        store_name_tokens = np.array([self.vocabs["store_name"].encode(e['store_name']) for e in events], dtype=np.int64)
        store_tag_tokens = np.array([self.vocabs["store_tag"].encode(e['primary_tag_name']) for e in events], dtype=np.int64)
        store_category_tokens = np.array([self.vocabs["store_category"].encode(e['primary_category_name']) for e in events], dtype=np.int64)
        
        cont_feats = np.array([[float(e.get('event_rank', 0.0)), min(float(e.get('seconds_from_prev', 0.0)), 3600.0), 
                                float(e.get('is_nv_store', 0))] for e in events], dtype=np.float32)
        
        return {
            "event_tokens": event_tokens, "feature_tokens": feature_tokens, "store_tokens": store_tokens,
            "store_name_tokens": store_name_tokens, "store_tag_tokens": store_tag_tokens,
            "store_category_tokens": store_category_tokens, "cont_feats": cont_feats,
            "seq_len": seq_len, "label": float(s["label"]), "session_id": s["session_id"],
            "event_types": [e['event_norm'] for e in events],
            "store_ids": [e['store_id'] for e in events]
        }

def collate_fn(batch, pad_idx=0):
    max_seq = max(b["seq_len"] for b in batch)
    def pad_arr(arr, target_len, pad_value=0):
        if arr.shape[0] >= target_len:
            return arr[:target_len]
        pad_shape = (target_len - arr.shape[0],) + arr.shape[1:]
        return np.concatenate([arr, np.full(pad_shape, pad_value, dtype=arr.dtype)], axis=0)
    
    return {
        "event_tokens": torch.tensor(np.stack([pad_arr(b["event_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "feature_tokens": torch.tensor(np.stack([pad_arr(b["feature_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "store_tokens": torch.tensor(np.stack([pad_arr(b["store_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "store_name_tokens": torch.tensor(np.stack([pad_arr(b["store_name_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "store_tag_tokens": torch.tensor(np.stack([pad_arr(b["store_tag_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "store_category_tokens": torch.tensor(np.stack([pad_arr(b["store_category_tokens"], max_seq, pad_idx) for b in batch]), dtype=torch.long),
        "cont_feats": torch.tensor(np.stack([pad_arr(b["cont_feats"], max_seq, 0.0) for b in batch]), dtype=torch.float32),
        "attention_mask": torch.tensor((np.stack([pad_arr(b["event_tokens"], max_seq, pad_idx) for b in batch]) != pad_idx).astype(np.int64), dtype=torch.long),
        "labels": torch.tensor([b["label"] for b in batch], dtype=torch.float32),
        "session_ids": [b["session_id"] for b in batch],
        "event_types": [b["event_types"] for b in batch],
        "store_ids": [b["store_ids"] for b in batch]
    }

class SessionTransformer(nn.Module):
    def __init__(self, event_vocab_size, feature_vocab_size, store_vocab_size,
                 store_name_vocab_size, store_tag_vocab_size, store_category_vocab_size,
                 d_model=192, nhead=6, num_layers=3, dim_feedforward=512, dropout=0.1, max_seq_len=100, use_cls_token=True):
        super().__init__()
        self.d_model, self.max_seq_len, self.use_cls_token = d_model, max_seq_len, use_cls_token
        
        emb_dim = d_model // 7
        self.event_emb = nn.Embedding(event_vocab_size, emb_dim, padding_idx=0)
        self.feature_emb = nn.Embedding(feature_vocab_size, emb_dim, padding_idx=0)
        self.store_emb = nn.Embedding(store_vocab_size, emb_dim, padding_idx=0)
        self.store_name_emb = nn.Embedding(store_name_vocab_size, emb_dim, padding_idx=0)
        self.store_tag_emb = nn.Embedding(store_tag_vocab_size, emb_dim, padding_idx=0)
        self.store_category_emb = nn.Embedding(store_category_vocab_size, emb_dim, padding_idx=0)
        self.cont_proj = nn.Linear(3, emb_dim)
        self.input_proj = nn.Linear(emb_dim * 7, d_model)
        self.pos_emb = nn.Embedding(max_seq_len + (1 if use_cls_token else 0), d_model)
        
        if use_cls_token:
            self.cls_token = nn.Parameter(torch.randn(1, 1, d_model))
        
        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=nhead, dim_feedforward=dim_feedforward,
                                                   dropout=dropout, activation="gelu", batch_first=True)
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        
        self.classifier = nn.Sequential(nn.LayerNorm(d_model), nn.Linear(d_model, d_model // 2),
                                       nn.GELU(), nn.Dropout(dropout), nn.Linear(d_model // 2, 1))
    
    def forward(self, event_tokens, feature_tokens, store_tokens, store_name_tokens,
                store_tag_tokens, store_category_tokens, cont_feats, attention_mask):
        B, S = event_tokens.shape
        
        x = torch.cat([self.event_emb(event_tokens), self.feature_emb(feature_tokens), self.store_emb(store_tokens),
                      self.store_name_emb(store_name_tokens), self.store_tag_emb(store_tag_tokens),
                      self.store_category_emb(store_category_tokens), self.cont_proj(cont_feats)], dim=-1)
        x = self.input_proj(x)
        
        if self.use_cls_token:
            x = torch.cat([self.cls_token.expand(B, -1, -1), x], dim=1)
            attn_mask = torch.cat([torch.ones((B, 1), dtype=attention_mask.dtype, device=attention_mask.device), attention_mask], dim=1)
        else:
            attn_mask = attention_mask
        
        x = x + self.pos_emb(torch.arange(x.shape[1], device=x.device).unsqueeze(0).repeat(B, 1))
        x = self.transformer(x, src_key_padding_mask=(attn_mask == 0))
        
        pooled = x[:, 0, :] if self.use_cls_token else (x * attn_mask.unsqueeze(-1).to(x.dtype)).sum(dim=1) / (attn_mask.sum(dim=1, keepdim=True) + 1e-8)
        return self.classifier(pooled).squeeze(-1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------

def train_epoch(model, loader, optimizer):
    model.train()
    losses, preds, trues = [], [], []
    for batch in tqdm(loader, desc="Train"):
        optimizer.zero_grad()
        logits = model(batch["event_tokens"].to(device), batch["feature_tokens"].to(device),
                      batch["store_tokens"].to(device), batch["store_name_tokens"].to(device),
                      batch["store_tag_tokens"].to(device), batch["store_category_tokens"].to(device),
                      batch["cont_feats"].to(device), batch["attention_mask"].to(device))
        loss = F_torch.binary_cross_entropy_with_logits(logits, batch["labels"].to(device))
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        losses.append(loss.item())
        preds.extend(torch.sigmoid(logits).detach().cpu().numpy())
        trues.extend(batch["labels"].numpy())
    return {"loss": np.mean(losses), "auc": roc_auc_score(trues, preds) if len(set(trues)) > 1 else 0.5}

def eval_epoch(model, loader):
    model.eval()
    preds, trues = [], []
    with torch.no_grad():
        for batch in tqdm(loader, desc="Eval"):
            logits = model(batch["event_tokens"].to(device), batch["feature_tokens"].to(device),
                          batch["store_tokens"].to(device), batch["store_name_tokens"].to(device),
                          batch["store_tag_tokens"].to(device), batch["store_category_tokens"].to(device),
                          batch["cont_feats"].to(device), batch["attention_mask"].to(device))
            preds.extend(torch.sigmoid(logits).cpu().numpy())
            trues.extend(batch["labels"].numpy())
    return {"auc": roc_auc_score(trues, preds) if len(set(trues)) > 1 else 0.5}

# COMMAND ----------

labels = pdf_sessions['label'].values
train_pdf, test_pdf = train_test_split(pdf_sessions, test_size=0.2, random_state=SEED, stratify=labels)
train_pdf, val_pdf = train_test_split(train_pdf, test_size=0.15, random_state=SEED, stratify=train_pdf['label'].values)

print(f"Train: {len(train_pdf):,} | Val: {len(val_pdf):,} | Test: {len(test_pdf):,}")

train_ds = SessionDataset(train_pdf, vocabs)
val_ds = SessionDataset(val_pdf, vocabs)
test_ds = SessionDataset(test_pdf, vocabs)

train_loader = DataLoader(train_ds, batch_size=64, shuffle=True, collate_fn=collate_fn)
val_loader = DataLoader(val_ds, batch_size=128, shuffle=False, collate_fn=collate_fn)
test_loader = DataLoader(test_ds, batch_size=128, shuffle=False, collate_fn=collate_fn)

# COMMAND ----------

model = SessionTransformer(
    event_vocab_size=len(vocabs["event"]), feature_vocab_size=len(vocabs["feature"]),
    store_vocab_size=len(vocabs["store"]), store_name_vocab_size=len(vocabs["store_name"]),
    store_tag_vocab_size=len(vocabs["store_tag"]), store_category_vocab_size=len(vocabs["store_category"]),
    d_model=192, nhead=6, num_layers=3
).to(device)

print(f"✓ Parameters: {sum(p.numel() for p in model.parameters()):,}")

optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4, weight_decay=1e-5)
best_auc = 0.0

for epoch in range(1, 5):
    print(f"\nEpoch {epoch}/5")
    train_res = train_epoch(model, train_loader, optimizer)
    val_res = eval_epoch(model, val_loader)
    print(f"Train Loss: {train_res['loss']:.4f} | Train AUC: {train_res['auc']:.4f} | Val AUC: {val_res['auc']:.4f}")
    if val_res['auc'] > best_auc:
        best_auc = val_res['auc']
        print(f"✓ Best (AUC: {val_res['auc']:.4f})")

print(f"\n✅ Training Complete! Best Val AUC: {best_auc:.4f} (NO funnel events)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Attribution Analysis - Extract Gradient-Based Importance

# COMMAND ----------

print("\n" + "="*80)
print("GRADIENT-BASED ATTRIBUTION ANALYSIS")
print("="*80)
print("\n🎯 Computing gradient × input for event attribution...")

model.train()  # Enable gradients
grad_attributions = []
samples_processed = 0

for batch in tqdm(test_loader, desc="Gradient Attribution"):
    if samples_processed >= 200:
        break
    
    event_tokens = batch["event_tokens"].to(device)
    feature_tokens = batch["feature_tokens"].to(device)
    store_tokens = batch["store_tokens"].to(device)
    store_name_tokens = batch["store_name_tokens"].to(device)
    store_tag_tokens = batch["store_tag_tokens"].to(device)
    store_category_tokens = batch["store_category_tokens"].to(device)
    cont_feats = batch["cont_feats"].to(device)
    attn_mask = batch["attention_mask"].to(device)
    
    B, S = event_tokens.shape
    
    # Get embeddings
    e = model.event_emb(event_tokens)
    f = model.feature_emb(feature_tokens)
    s = model.store_emb(store_tokens)
    sn = model.store_name_emb(store_name_tokens)
    st = model.store_tag_emb(store_tag_tokens)
    sc = model.store_category_emb(store_category_tokens)
    c = model.cont_proj(cont_feats)
    
    x = torch.cat([e, f, s, sn, st, sc, c], dim=-1)
    x = model.input_proj(x)
    x.requires_grad_(True)
    x.retain_grad()  # ✅ Retain gradients for non-leaf tensor
    
    # Add CLS
    if model.use_cls_token:
        cls = model.cls_token.expand(B, -1, -1)
        x_with_cls = torch.cat([cls, x], dim=1)
        attn_mask_ext = torch.cat([torch.ones((B, 1), dtype=attn_mask.dtype, device=attn_mask.device), attn_mask], dim=1)
    else:
        x_with_cls = x
        attn_mask_ext = attn_mask
    
    # Positional encoding
    seq_len = x_with_cls.shape[1]
    pos_idx = torch.arange(seq_len, device=x_with_cls.device).unsqueeze(0).repeat(B, 1)
    x_with_cls = x_with_cls + model.pos_emb(pos_idx)
    
    # Transformer
    x_encoded = model.transformer(x_with_cls, src_key_padding_mask=(attn_mask_ext == 0))
    
    # Pooling
    if model.use_cls_token:
        pooled = x_encoded[:, 0, :]
    else:
        mask = attn_mask_ext.unsqueeze(-1).to(x_encoded.dtype)
        pooled = (x_encoded * mask).sum(dim=1) / (mask.sum(dim=1) + 1e-8)
    
    # Logits
    logits = model.classifier(pooled).squeeze(-1)
    
    # Backprop for each sample
    for i in range(B):
        if samples_processed >= 200:
            break
        
        model.zero_grad()
        if x.grad is not None:
            x.grad.zero_()  # Clear previous gradients
        
        logits[i].backward(retain_graph=True)
        
        if x.grad is not None:
            grads = x.grad[i].detach().cpu().numpy()
            inputs = x[i].detach().cpu().numpy()
            saliency = np.abs((grads * inputs).sum(axis=-1))
            saliency = saliency / (saliency.sum() + 1e-12)
            
            seq_len_i = attn_mask[i].sum().item()
            for event_idx in range(seq_len_i):
                grad_attributions.append({
                    'session_id': batch["session_ids"][i],
                    'label': batch["labels"][i].item(),
                    'event_position': event_idx,
                    'gradient_attribution': saliency[event_idx],
                    'event_type': batch["event_types"][i][event_idx],
                    'store_id': batch["store_ids"][i][event_idx]
                })
            
            x.grad.zero_()
        
        samples_processed += 1

df_grad_attribution = pd.DataFrame(grad_attributions)
print(f"\n✓ Computed attribution for {len(df_grad_attribution):,} events from {samples_processed} sessions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Event Type Attribution Analysis

# COMMAND ----------

print("\n" + "="*80)
print("EVENT TYPE ATTRIBUTION")
print("="*80)

# Attribution already has event_type and store_id from the batch
df_grad_full = df_grad_attribution

# Event type attribution
event_attr = df_grad_full.groupby(['label', 'event_type'])['gradient_attribution'].mean().reset_index()
event_pivot = event_attr.pivot(index='event_type', columns='label', values='gradient_attribution')
event_pivot.columns = ['not_converted', 'converted']
event_pivot['attribution_lift'] = event_pivot['converted'] - event_pivot['not_converted']
event_pivot = event_pivot.sort_values('attribution_lift', ascending=False)

print("\n**Top 20 Events by Attribution Lift:**")
display(event_pivot.head(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Position-Based Attribution

# COMMAND ----------

print("\n" + "="*80)
print("POSITION-BASED ATTRIBUTION")
print("="*80)

position_bins = pd.cut(df_grad_full['event_position'], bins=[0, 5, 10, 20, 50, 100], labels=['1-5', '6-10', '11-20', '21-50', '50+'])
df_grad_full['position_bin'] = position_bins

pos_attr = df_grad_full.groupby(['label', 'position_bin'])['gradient_attribution'].mean().reset_index()
pos_pivot = pos_attr.pivot(index='position_bin', columns='label', values='gradient_attribution')
pos_pivot.columns = ['not_converted', 'converted']
pos_pivot['attribution_lift'] = pos_pivot['converted'] - pos_pivot['not_converted']

print("\n**Attribution by Event Position:**")
display(pos_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store-Level Attribution

# COMMAND ----------

print("\n" + "="*80)
print("STORE-LEVEL ATTRIBUTION")
print("="*80)

store_attr = df_grad_full.groupby(['label', 'store_id'])['gradient_attribution'].agg(['mean', 'count']).reset_index()
store_attr = store_attr[store_attr['count'] >= 10]

store_converted = store_attr[store_attr['label'] == 1.0].nlargest(10, 'mean')
print("\n**Top 10 Stores in Converted Sessions:**")
display(store_converted)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualizations

# COMMAND ----------

fig, axes = plt.subplots(2, 2, figsize=(16, 12))

# Plot 1: Event type attribution lift
top_events = event_pivot.head(20)
colors = ['green' if x > 0 else 'red' for x in top_events['attribution_lift']]

axes[0, 0].barh(range(len(top_events)), top_events['attribution_lift'], color=colors, alpha=0.7)
axes[0, 0].set_yticks(range(len(top_events)))
axes[0, 0].set_yticklabels(top_events.index)
axes[0, 0].set_xlabel('Attribution Lift', fontsize=11, fontweight='bold')
axes[0, 0].set_title('**Top Events by Attribution Lift**', fontsize=12, fontweight='bold')
axes[0, 0].axvline(x=0, color='black', linestyle='--', linewidth=1)
axes[0, 0].invert_yaxis()
axes[0, 0].grid(axis='x', alpha=0.3)

# Plot 2: Position-based
x_pos = np.arange(len(pos_pivot))
width = 0.35
axes[0, 1].bar(x_pos - width/2, pos_pivot['converted'], width, label='Converted', color='#2ecc71', alpha=0.8)
axes[0, 1].bar(x_pos + width/2, pos_pivot['not_converted'], width, label='Not Converted', color='#e74c3c', alpha=0.8)
axes[0, 1].set_xlabel('Event Position', fontsize=11, fontweight='bold')
axes[0, 1].set_ylabel('Avg Attribution', fontsize=11, fontweight='bold')
axes[0, 1].set_title('**Attribution by Event Position**', fontsize=12, fontweight='bold')
axes[0, 1].set_xticks(x_pos)
axes[0, 1].set_xticklabels(pos_pivot.index)
axes[0, 1].legend()
axes[0, 1].grid(axis='y', alpha=0.3)

# Plot 3: Converted vs not converted
top_events_comp = event_pivot.head(15)
x_comp = np.arange(len(top_events_comp))

axes[1, 0].barh(x_comp - width/2, top_events_comp['converted'], width, label='Converted', color='#2ecc71', alpha=0.8)
axes[1, 0].barh(x_comp + width/2, top_events_comp['not_converted'], width, label='Not Converted', color='#e74c3c', alpha=0.8)
axes[1, 0].set_yticks(x_comp)
axes[1, 0].set_yticklabels(top_events_comp.index)
axes[1, 0].set_xlabel('Avg Attribution', fontsize=11, fontweight='bold')
axes[1, 0].set_title('**Event Attribution: Converted vs Not Converted**', fontsize=12, fontweight='bold')
axes[1, 0].legend()
axes[1, 0].invert_yaxis()
axes[1, 0].grid(axis='x', alpha=0.3)

# Plot 4: Heatmap
top_event_types = event_pivot.head(10).index.tolist()
heatmap_data = df_grad_full[df_grad_full['event_type'].isin(top_event_types)]
heatmap_data = heatmap_data[heatmap_data['event_position'] < 20]

pivot_heatmap = heatmap_data.pivot_table(
    index='event_type', columns='event_position', values='gradient_attribution', aggfunc='mean'
)

sns.heatmap(pivot_heatmap, cmap='YlOrRd', ax=axes[1, 1], cbar_kws={'label': 'Attribution'})
axes[1, 1].set_xlabel('Event Position', fontsize=11, fontweight='bold')
axes[1, 1].set_ylabel('Event Type', fontsize=11, fontweight='bold')
axes[1, 1].set_title('**Attribution Heatmap: Event Type × Position**', fontsize=12, fontweight='bold')

plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results to Snowflake

# COMMAND ----------

print("Saving results to Snowflake...")

# Event type attribution
event_importance_spark = spark.createDataFrame(event_pivot.reset_index())
event_importance_spark = event_importance_spark.withColumn("created_at", F.current_timestamp())

event_importance_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.transformer_event_attribution_no_funnel") \
    .mode("overwrite") \
    .save()

print("✓ Saved event attribution to proddb.fionafan.transformer_event_attribution_no_funnel")

# Position attribution
pos_importance_spark = spark.createDataFrame(pos_pivot.reset_index())
pos_importance_spark = pos_importance_spark.withColumn("created_at", F.current_timestamp())

pos_importance_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.transformer_position_attribution_no_funnel") \
    .mode("overwrite") \
    .save()

print("✓ Saved position attribution to proddb.fionafan.transformer_position_attribution_no_funnel")

# Store attribution
store_importance_spark = spark.createDataFrame(store_attr)
store_importance_spark = store_importance_spark.withColumn("created_at", F.current_timestamp())

store_importance_spark.write.format("snowflake") \
    .options(**OPTIONS) \
    .option("dbtable", "proddb.fionafan.transformer_store_attribution_no_funnel") \
    .mode("overwrite") \
    .save()

print("✓ Saved store attribution to proddb.fionafan.transformer_store_attribution_no_funnel")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*80)
print("✅ ANALYSIS COMPLETE!")
print("="*80)

print(f"\n**Model Performance:**")
print(f"  Best Val AUC: {best_auc:.4f} (NO funnel events - true predictive power)")

print(f"\n**Top 5 Most Important Events:**")
for idx, (event_type, row) in enumerate(event_pivot.head(5).iterrows(), 1):
    print(f"  {idx}. {event_type:40} | Lift: {row['attribution_lift']:+.4f}")

print(f"\n**Most Important Positions:**")
for pos, row in pos_pivot.iterrows():
    print(f"  Position {pos:8} | Lift: {row['attribution_lift']:+.4f}")

print(f"\n**Results Saved to Snowflake:**")
print(f"  - proddb.fionafan.transformer_event_attribution_no_funnel")
print(f"  - proddb.fionafan.transformer_position_attribution_no_funnel")
print(f"  - proddb.fionafan.transformer_store_attribution_no_funnel")

# COMMAND ----------



