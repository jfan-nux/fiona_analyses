"""
Session Conversion Prediction with Transformer - NO FUNNEL EVENTS (No Data Leakage)

This version EXCLUDES funnel events that would leak conversion information:
- Excludes: add_item, cart_page_load, checkout_page_load, place_order, checkout_success
- Includes: impressions, clicks, store_page_load, search, tab switches, etc.

Uses standard nn.TransformerEncoder with hook-based attention extraction for attribution.
"""

import os
import sys
import math
import random
from typing import List, Dict, Tuple
from collections import defaultdict

import numpy as np
import pandas as pd
from tqdm.auto import tqdm

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader

from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append('/Users/fiona.fan/Documents/fiona_analyses')
from utils.snowflake_connection import SnowflakeHook

# Random seeds
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
torch.manual_seed(SEED)
if torch.cuda.is_available():
    torch.cuda.manual_seed_all(SEED)

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")


# ============================================================================
# Vocabulary
# ============================================================================
class Vocab:
    def __init__(self, min_freq=2, unk_token="<UNK>"):
        self.min_freq = min_freq
        self.unk_token = unk_token
        self.pad_token = "<PAD>"
        self.freqs = {}
        self.token_to_idx = {self.pad_token: 0, self.unk_token: 1}
        self.idx_to_token = [self.pad_token, self.unk_token]
    
    def add(self, token):
        self.freqs[token] = self.freqs.get(token, 0) + 1
    
    def build(self):
        for token, freq in sorted(self.freqs.items(), key=lambda x: (-x[1], x[0])):
            if token in (self.pad_token, self.unk_token):
                continue
            if freq >= self.min_freq:
                self.token_to_idx[token] = len(self.idx_to_token)
                self.idx_to_token.append(token)
        print(f"  Built vocab: {len(self.idx_to_token)} tokens")
    
    def __len__(self):
        return len(self.idx_to_token)
    
    def encode(self, token):
        return self.token_to_idx.get(token, self.token_to_idx[self.unk_token])


# ============================================================================
# Dataset
# ============================================================================
class SessionDataset(Dataset):
    def __init__(self, sessions, event_vocab, feature_vocab, store_vocab, 
                 store_name_vocab, store_tag_vocab, store_category_vocab, max_seq_len=100):
        self.sessions = sessions
        self.event_vocab = event_vocab
        self.feature_vocab = feature_vocab
        self.store_vocab = store_vocab
        self.store_name_vocab = store_name_vocab
        self.store_tag_vocab = store_tag_vocab
        self.store_category_vocab = store_category_vocab
        self.max_seq_len = max_seq_len
    
    def __len__(self):
        return len(self.sessions)
    
    def __getitem__(self, idx):
        s = self.sessions[idx]
        events = s["events"][:self.max_seq_len]
        seq_len = len(events)
        
        event_tokens = np.array([self.event_vocab.encode(e.get("event_norm", "UNKNOWN")) for e in events], dtype=np.int64)
        feature_tokens = np.array([self.feature_vocab.encode(e.get("discovery_feature_norm", "UNKNOWN")) for e in events], dtype=np.int64)
        store_tokens = np.array([self.store_vocab.encode(str(e.get("store_id", "UNKNOWN"))) for e in events], dtype=np.int64)
        store_name_tokens = np.array([self.store_name_vocab.encode(e.get("store_name", "NO_STORE")) for e in events], dtype=np.int64)
        store_tag_tokens = np.array([self.store_tag_vocab.encode(e.get("primary_tag_name", "NO_STORE")) for e in events], dtype=np.int64)
        store_category_tokens = np.array([self.store_category_vocab.encode(e.get("primary_category_name", "NO_STORE")) for e in events], dtype=np.int64)
        
        cont_feats = []
        for e in events:
            event_rank = float(e.get("event_rank") or 0.0)
            secs_prev = min(float(e.get("seconds_from_prev") or 0.0), 3600.0)
            is_nv = 1.0 if e.get("is_nv_store") else 0.0
            cont_feats.append([event_rank, secs_prev, is_nv])
        cont_feats = np.array(cont_feats, dtype=np.float32)
        
        return {
            "event_tokens": event_tokens, "feature_tokens": feature_tokens, "store_tokens": store_tokens,
            "store_name_tokens": store_name_tokens, "store_tag_tokens": store_tag_tokens, 
            "store_category_tokens": store_category_tokens, "cont_feats": cont_feats,
            "seq_len": seq_len, "label": float(s.get("label", 0)), "session_id": s.get("session_id"),
            "store_ids": [e.get("store_id") for e in events], "event_types": [e.get("event_norm") for e in events]
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
        "store_ids": [b["store_ids"] for b in batch],
        "event_types": [b["event_types"] for b in batch]
    }


# ============================================================================
# Transformer Model (using nn.TransformerEncoder)
# ============================================================================
class SessionTransformer(nn.Module):
    def __init__(self, event_vocab_size, feature_vocab_size, store_vocab_size,
                 store_name_vocab_size, store_tag_vocab_size, store_category_vocab_size,
                 cont_feat_dim=3, d_model=192, nhead=6, num_layers=3,
                 dim_feedforward=512, dropout=0.1, max_seq_len=100, use_cls_token=True):
        super().__init__()
        self.d_model = d_model
        self.max_seq_len = max_seq_len
        self.use_cls_token = use_cls_token
        
        # Embeddings
        emb_dim = d_model // 7
        self.event_emb = nn.Embedding(event_vocab_size, emb_dim, padding_idx=0)
        self.feature_emb = nn.Embedding(feature_vocab_size, emb_dim, padding_idx=0)
        self.store_emb = nn.Embedding(store_vocab_size, emb_dim, padding_idx=0)
        self.store_name_emb = nn.Embedding(store_name_vocab_size, emb_dim, padding_idx=0)
        self.store_tag_emb = nn.Embedding(store_tag_vocab_size, emb_dim, padding_idx=0)
        self.store_category_emb = nn.Embedding(store_category_vocab_size, emb_dim, padding_idx=0)
        self.cont_proj = nn.Linear(cont_feat_dim, emb_dim)
        self.input_proj = nn.Linear(emb_dim * 7, d_model)
        
        # Positional encoding
        self.pos_emb = nn.Embedding(max_seq_len + (1 if use_cls_token else 0), d_model)
        
        # CLS token
        if use_cls_token:
            self.cls_token = nn.Parameter(torch.randn(1, 1, d_model))
        
        # Transformer encoder (standard PyTorch)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=nhead, dim_feedforward=dim_feedforward,
            dropout=dropout, activation="gelu", batch_first=True
        )
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        
        # Classification head
        self.classifier = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model // 2, 1)
        )
        
        # Storage for attention weights (set by hooks)
        self.attention_weights = []
    
    def forward(self, event_tokens, feature_tokens, store_tokens, store_name_tokens,
                store_tag_tokens, store_category_tokens, cont_feats, attention_mask):
        B, S = event_tokens.shape
        
        # Embeddings
        e = self.event_emb(event_tokens)
        f = self.feature_emb(feature_tokens)
        s = self.store_emb(store_tokens)
        sn = self.store_name_emb(store_name_tokens)
        st = self.store_tag_emb(store_tag_tokens)
        sc = self.store_category_emb(store_category_tokens)
        c = self.cont_proj(cont_feats)
        
        x = torch.cat([e, f, s, sn, st, sc, c], dim=-1)
        x = self.input_proj(x)
        
        # Add CLS token
        if self.use_cls_token:
            cls = self.cls_token.expand(B, -1, -1)
            x = torch.cat([cls, x], dim=1)
            seq_len = S + 1
            attn_mask = torch.cat([torch.ones((B, 1), dtype=attention_mask.dtype, device=attention_mask.device), attention_mask], dim=1)
        else:
            seq_len = S
            attn_mask = attention_mask
        
        # Positional encoding
        pos_idx = torch.arange(seq_len, device=x.device).unsqueeze(0).repeat(B, 1)
        x = x + self.pos_emb(pos_idx)
        
        # Transformer
        src_key_padding_mask = (attn_mask == 0)
        x_encoded = self.transformer(x, src_key_padding_mask=src_key_padding_mask)
        
        # Pooling
        if self.use_cls_token:
            pooled = x_encoded[:, 0, :]
        else:
            mask = attn_mask.unsqueeze(-1).to(x_encoded.dtype)
            pooled = (x_encoded * mask).sum(dim=1) / (mask.sum(dim=1) + 1e-8)
        
        logits = self.classifier(pooled).squeeze(-1)
        return logits


# Hook-based attention extraction
def register_attention_hooks(model):
    """Register hooks to capture attention weights from MultiheadAttention modules"""
    attention_weights = []
    hooks = []
    
    def make_hook(module_name):
        def hook(module, input, output):
            # output[1] contains attention weights when need_weights=True
            if len(output) > 1 and output[1] is not None:
                attention_weights.append(output[1].detach().cpu())
        return hook
    
    for name, module in model.transformer.named_modules():
        if isinstance(module, nn.MultiheadAttention):
            hooks.append(module.register_forward_hook(make_hook(name)))
    
    return attention_weights, hooks


# ============================================================================
# Training
# ============================================================================
def train_epoch(model, dataloader, optimizer, device):
    model.train()
    losses, preds, trues = [], [], []
    
    for batch in tqdm(dataloader, desc="Training"):
        optimizer.zero_grad()
        logits = model(
            batch["event_tokens"].to(device), batch["feature_tokens"].to(device),
            batch["store_tokens"].to(device), batch["store_name_tokens"].to(device),
            batch["store_tag_tokens"].to(device), batch["store_category_tokens"].to(device),
            batch["cont_feats"].to(device), batch["attention_mask"].to(device)
        )
        loss = F.binary_cross_entropy_with_logits(logits, batch["labels"].to(device))
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        optimizer.step()
        
        losses.append(loss.item())
        preds.extend(torch.sigmoid(logits).detach().cpu().numpy())
        trues.extend(batch["labels"].numpy())
    
    return {"loss": np.mean(losses), "auc": roc_auc_score(trues, preds) if len(set(trues)) > 1 else 0.5}


def eval_epoch(model, dataloader, device):
    model.eval()
    preds, trues = [], []
    
    with torch.no_grad():
        for batch in tqdm(dataloader, desc="Evaluating"):
            logits = model(
                batch["event_tokens"].to(device), batch["feature_tokens"].to(device),
                batch["store_tokens"].to(device), batch["store_name_tokens"].to(device),
                batch["store_tag_tokens"].to(device), batch["store_category_tokens"].to(device),
                batch["cont_feats"].to(device), batch["attention_mask"].to(device)
            )
            preds.extend(torch.sigmoid(logits).cpu().numpy())
            trues.extend(batch["labels"].numpy())
    
    return {"auc": roc_auc_score(trues, preds) if len(set(trues)) > 1 else 0.5}


# ============================================================================
# Main
# ============================================================================
def main():
    print("\n" + "="*80)
    print("TRANSFORMER ATTRIBUTION - NO FUNNEL EVENTS (No Data Leakage)")
    print("="*80)
    
    os.makedirs('outputs', exist_ok=True)
    os.makedirs('plots', exist_ok=True)
    
    # Load data - EXCLUDE FUNNEL EVENTS
    print("\n📊 Loading events (EXCLUDING funnel events to prevent data leakage)...")
    
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
    ),
    event_data AS (
        SELECT
            e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM,
            e.TIMESTAMP AS event_ts,
            COALESCE(e.EVENT_TYPE, 'UNKNOWN') AS event_type_norm,
            COALESCE(e.EVENT, 'UNKNOWN') AS event_norm,
            COALESCE(e.DISCOVERY_FEATURE, 'UNKNOWN') AS discovery_feature_norm,
            e.STORE_ID, e.EVENT_RANK, sc.converted,
            CASE WHEN ds.NV_ORG IS NOT NULL OR ds.NV_VERTICAL_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_nv_store,
            COALESCE(ds.NAME, 'NO_STORE') AS store_name,
            COALESCE(ds.PRIMARY_TAG_NAME, 'NO_STORE') AS primary_tag_name,
            COALESCE(ds.PRIMARY_CATEGORY_NAME, 'NO_STORE') AS primary_category_name,
            ROW_NUMBER() OVER (PARTITION BY e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM ORDER BY e.TIMESTAMP) AS event_sequence_num,
            LAG(e.TIMESTAMP) OVER (PARTITION BY e.USER_ID, e.DD_DEVICE_ID, e.EVENT_DATE, e.SESSION_NUM ORDER BY e.TIMESTAMP) AS prev_event_ts
        FROM proddb.fionafan.all_user_sessions_with_events e
        INNER JOIN session_conversions sc
            ON e.USER_ID = sc.USER_ID AND e.DD_DEVICE_ID = sc.DD_DEVICE_ID
            AND e.EVENT_DATE = sc.EVENT_DATE AND e.SESSION_NUM = sc.SESSION_NUM
        LEFT JOIN edw.merchant.dimension_store ds ON e.STORE_ID = ds.STORE_ID
        WHERE e.EVENT_TYPE NOT ILIKE '%error%' AND e.EVENT NOT ILIKE '%error%'
          -- EXCLUDE ALL FUNNEL EVENTS from sequence (but we still have conversion label)
          AND e.EVENT_TYPE != 'funnel'
    )
    SELECT *, DATEDIFF(second, prev_event_ts, event_ts) AS seconds_from_prev
    FROM event_data
    ORDER BY USER_ID, DD_DEVICE_ID, EVENT_DATE, SESSION_NUM, event_sequence_num
    """
    
    hook = SnowflakeHook()
    df_events = hook.query_snowflake(query, method='pandas')
    print(f"✓ Loaded {len(df_events):,} events (funnel events excluded)")
    
    # Build vocabularies
    print("\n📚 Building vocabularies...")
    vocabs = {}
    for name, col in [("event", "event_norm"), ("feature", "discovery_feature_norm"), 
                      ("store", "store_id"), ("store_name", "store_name"),
                      ("store_tag", "primary_tag_name"), ("store_category", "primary_category_name")]:
        vocab = Vocab(min_freq=3)
        for val in df_events[col]:
            vocab.add(str(val) if name == "store" else val)
        vocab.build()
        vocabs[name] = vocab
    
    # Group into sessions
    print("\n🔄 Grouping into sessions...")
    sessions = []
    for (user_id, device_id, event_date, session_num), group in df_events.groupby(
        ['user_id', 'dd_device_id', 'event_date', 'session_num']
    ):
        events = []
        for _, row in group.iterrows():
            events.append({
                'event_norm': row['event_norm'], 'discovery_feature_norm': row['discovery_feature_norm'],
                'store_id': row['store_id'], 'store_name': row['store_name'],
                'primary_tag_name': row['primary_tag_name'], 'primary_category_name': row['primary_category_name'],
                'event_rank': row['event_rank'], 'seconds_from_prev': row['seconds_from_prev'] if pd.notna(row['seconds_from_prev']) else 0.0,
                'is_nv_store': row['is_nv_store']
            })
        sessions.append({'session_id': f"{user_id}_{device_id}_{event_date}_{session_num}", 'events': events, 'label': group['converted'].iloc[0]})
    
    print(f"✓ Created {len(sessions):,} sessions | Conversion rate: {np.mean([s['label'] for s in sessions]):.2%}")
    
    # Split data
    labels = [s['label'] for s in sessions]
    train_sessions, test_sessions = train_test_split(sessions, test_size=0.2, random_state=SEED, stratify=labels)
    train_sessions, val_sessions = train_test_split(train_sessions, test_size=0.15, random_state=SEED, stratify=[s['label'] for s in train_sessions])
    
    print(f"\n✂️  Train: {len(train_sessions):,} | Val: {len(val_sessions):,} | Test: {len(test_sessions):,}")
    
    # Create datasets
    train_ds = SessionDataset(train_sessions, vocabs["event"], vocabs["feature"], vocabs["store"], 
                               vocabs["store_name"], vocabs["store_tag"], vocabs["store_category"])
    val_ds = SessionDataset(val_sessions, vocabs["event"], vocabs["feature"], vocabs["store"],
                             vocabs["store_name"], vocabs["store_tag"], vocabs["store_category"])
    test_ds = SessionDataset(test_sessions, vocabs["event"], vocabs["feature"], vocabs["store"],
                             vocabs["store_name"], vocabs["store_tag"], vocabs["store_category"])
    
    train_loader = DataLoader(train_ds, batch_size=64, shuffle=True, collate_fn=collate_fn)
    val_loader = DataLoader(val_ds, batch_size=128, shuffle=False, collate_fn=collate_fn)
    test_loader = DataLoader(test_ds, batch_size=128, shuffle=False, collate_fn=collate_fn)
    
    # Create model
    print("\n🏗️  Building Transformer...")
    model = SessionTransformer(
        event_vocab_size=len(vocabs["event"]), feature_vocab_size=len(vocabs["feature"]),
        store_vocab_size=len(vocabs["store"]), store_name_vocab_size=len(vocabs["store_name"]),
        store_tag_vocab_size=len(vocabs["store_tag"]), store_category_vocab_size=len(vocabs["store_category"]),
        d_model=192, nhead=6, num_layers=3, use_cls_token=True
    ).to(device)
    
    print(f"✓ Parameters: {sum(p.numel() for p in model.parameters()):,}")
    
    # Train
    print("\n🚀 Training...")
    optimizer = torch.optim.AdamW(model.parameters(), lr=3e-4, weight_decay=1e-5)
    best_val_auc = 0.0
    
    for epoch in range(1, 5):
        print(f"\n{'='*80}\nEpoch {epoch}/5\n{'='*80}")
        train_res = train_epoch(model, train_loader, optimizer, device)
        val_res = eval_epoch(model, val_loader, device)
        print(f"Train Loss: {train_res['loss']:.4f} | Train AUC: {train_res['auc']:.4f}")
        print(f"Val AUC: {val_res['auc']:.4f}")
        
        if val_res['auc'] > best_val_auc:
            best_val_auc = val_res['auc']
            torch.save(model.state_dict(), 'outputs/best_transformer_no_funnel.pt')
            print(f"✓ Saved (AUC: {val_res['auc']:.4f})")
    
    print(f"\n✅ Training Complete! Best Val AUC: {best_val_auc:.4f}")
    
    # ========================================================================
    # ATTRIBUTION ANALYSIS - Extract Attention Weights
    # ========================================================================
    print("\n" + "="*80)
    print("ATTENTION-BASED ATTRIBUTION ANALYSIS")
    print("="*80)
    
    # Load best model
    model.load_state_dict(torch.load('outputs/best_transformer_no_funnel.pt'))
    model.eval()
    
    # Create test dataset and loader
    test_ds = SessionDataset(test_sessions, vocabs["event"], vocabs["feature"], vocabs["store"],
                             vocabs["store_name"], vocabs["store_tag"], vocabs["store_category"])
    test_loader = DataLoader(test_ds, batch_size=128, shuffle=False, collate_fn=collate_fn)
    
    # Extract attention weights using hooks
    print("\n🔍 Extracting attention weights from transformer layers...")
    
    attributions = []
    num_samples = 1000
    samples_processed = 0
    
    for batch in tqdm(test_loader, desc="Extracting Attention"):
        if samples_processed >= num_samples:
            break
        
        # Register hooks to capture attention
        attention_weights = []
        hooks = []
        
        def make_hook():
            def hook(module, input, output):
                # nn.MultiheadAttention returns (attn_output, attn_output_weights)
                # But by default need_weights=False, so we need to monkey-patch
                pass
            return hook
        
        # Alternative: manually compute attention by accessing layer weights
        # For now, use a simpler gradient-based attribution approach
        
        with torch.no_grad():
            event_tokens = batch["event_tokens"].to(device)
            feature_tokens = batch["feature_tokens"].to(device)
            store_tokens = batch["store_tokens"].to(device)
            store_name_tokens = batch["store_name_tokens"].to(device)
            store_tag_tokens = batch["store_tag_tokens"].to(device)
            store_category_tokens = batch["store_category_tokens"].to(device)
            cont_feats = batch["cont_feats"].to(device)
            attn_mask = batch["attention_mask"].to(device)
            
            logits = model(event_tokens, feature_tokens, store_tokens, store_name_tokens,
                          store_tag_tokens, store_category_tokens, cont_feats, attn_mask)
            
            # For attribution, we'll use a gradient-based approach: gradient × input
            # This shows which events contribute most to the prediction
            
            batch_size = event_tokens.shape[0]
            
            for i in range(batch_size):
                if samples_processed >= num_samples:
                    break
                
                seq_len = attn_mask[i].sum().item()
                
                # Store event-level info
                for event_idx in range(seq_len):
                    attributions.append({
                        'session_id': batch["session_ids"][i],
                        'label': batch["labels"][i].item(),
                        'prediction': torch.sigmoid(logits[i]).item(),
                        'event_position': event_idx,
                        'event_type': batch["event_types"][i][event_idx],
                        'store_id': batch["store_ids"][i][event_idx]
                    })
                
                samples_processed += 1
    
    df_attribution = pd.DataFrame(attributions)
    
    # ========================================================================
    # Gradient-Based Attribution (More Reliable than Hook-Based)
    # ========================================================================
    print("\n🎯 Computing gradient-based attribution...")
    
    model.train()  # Enable gradients
    grad_attributions = []
    samples_processed = 0
    
    for batch in tqdm(test_loader, desc="Gradient Attribution"):
        if samples_processed >= 200:  # Smaller sample for gradient computation
            break
        
        event_tokens = batch["event_tokens"].to(device)
        feature_tokens = batch["feature_tokens"].to(device)
        store_tokens = batch["store_tokens"].to(device)
        store_name_tokens = batch["store_name_tokens"].to(device)
        store_tag_tokens = batch["store_tag_tokens"].to(device)
        store_category_tokens = batch["store_category_tokens"].to(device)
        cont_feats = batch["cont_feats"].to(device)
        attn_mask = batch["attention_mask"].to(device)
        
        # Enable gradient on embeddings
        event_tokens.requires_grad_(False)
        
        # Get embeddings with gradients
        B, S = event_tokens.shape
        
        # Forward pass through embeddings
        emb_dim = model.d_model // 7
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
        
        # Add CLS token
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
        src_key_padding_mask = (attn_mask_ext == 0)
        x_encoded = model.transformer(x_with_cls, src_key_padding_mask=src_key_padding_mask)
        
        # Pooling
        if model.use_cls_token:
            pooled = x_encoded[:, 0, :]
        else:
            mask = attn_mask_ext.unsqueeze(-1).to(x_encoded.dtype)
            pooled = (x_encoded * mask).sum(dim=1) / (mask.sum(dim=1) + 1e-8)
        
        # Logits
        logits = model.classifier(pooled).squeeze(-1)
        
        # Backprop to get gradients
        for i in range(B):
            if samples_processed >= 200:
                break
            
            model.zero_grad()
            if x.grad is not None:
                x.grad.zero_()  # Clear previous gradients
            
            logits[i].backward(retain_graph=True)
            
            # Get gradient of input embeddings
            if x.grad is not None:
                grads = x.grad[i].detach().cpu().numpy()  # (S, d_model)
                inputs = x[i].detach().cpu().numpy()
                
                # Gradient × Input (saliency)
                saliency = (grads * inputs).sum(axis=-1)  # (S,)
                saliency = np.abs(saliency)  # Take absolute value
                
                # Normalize
                saliency = saliency / (saliency.sum() + 1e-12)
                
                seq_len = attn_mask[i].sum().item()
                for event_idx in range(seq_len):
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
    
    # ========================================================================
    # Analyze Attribution at Multiple Granularities
    # ========================================================================
    print("\n" + "="*80)
    print("ATTRIBUTION ANALYSIS - EVENT TYPE IMPORTANCE")
    print("="*80)
    
    # 1. EVENT TYPE ATTRIBUTION
    event_attr = df_grad_attribution.groupby(['label', 'event_type'])['gradient_attribution'].mean().reset_index()
    event_pivot = event_attr.pivot(index='event_type', columns='label', values='gradient_attribution')
    event_pivot.columns = ['not_converted', 'converted']
    event_pivot['attribution_lift'] = event_pivot['converted'] - event_pivot['not_converted']
    event_pivot = event_pivot.sort_values('attribution_lift', ascending=False)
    
    print("\n**Top 15 Events by Attribution Lift** (converted - not_converted):")
    print(event_pivot.head(15).to_string())
    
    # 2. POSITION-BASED ATTRIBUTION
    print("\n" + "="*80)
    print("ATTRIBUTION ANALYSIS - POSITION IMPORTANCE")
    print("="*80)
    
    position_bins = pd.cut(df_grad_attribution['event_position'], bins=[0, 5, 10, 20, 50, 100], labels=['1-5', '6-10', '11-20', '21-50', '50+'])
    df_grad_attribution['position_bin'] = position_bins
    
    pos_attr = df_grad_attribution.groupby(['label', 'position_bin'])['gradient_attribution'].mean().reset_index()
    pos_pivot = pos_attr.pivot(index='position_bin', columns='label', values='gradient_attribution')
    pos_pivot.columns = ['not_converted', 'converted']
    pos_pivot['attribution_lift'] = pos_pivot['converted'] - pos_pivot['not_converted']
    
    print("\n**Attribution by Event Position:**")
    print(pos_pivot.to_string())
    
    # 3. STORE-LEVEL ATTRIBUTION
    print("\n" + "="*80)
    print("ATTRIBUTION ANALYSIS - TOP STORES")
    print("="*80)
    
    store_attr = df_grad_attribution.groupby(['label', 'store_id'])['gradient_attribution'].agg(['mean', 'count']).reset_index()
    store_attr = store_attr[store_attr['count'] >= 10]  # Filter to stores with enough events
    
    store_converted = store_attr[store_attr['label'] == 1.0].nlargest(10, 'mean')
    print("\n**Top 10 Stores in Converted Sessions:**")
    print(store_converted[['store_id', 'mean', 'count']].to_string())
    
    # ========================================================================
    # Visualizations
    # ========================================================================
    print("\n📊 Creating visualizations...")
    
    # Plot 1: Event Type Attribution Lift
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    top_events = event_pivot.head(20)
    colors = ['green' if x > 0 else 'red' for x in top_events['attribution_lift']]
    
    axes[0, 0].barh(range(len(top_events)), top_events['attribution_lift'], color=colors, alpha=0.7)
    axes[0, 0].set_yticks(range(len(top_events)))
    axes[0, 0].set_yticklabels(top_events.index)
    axes[0, 0].set_xlabel('Attribution Lift', fontsize=11, fontweight='bold')
    axes[0, 0].set_title('**Top Events by Attribution Lift**', fontsize=12, fontweight='bold')
    axes[0, 0].axvline(x=0, color='black', linestyle='--', linewidth=1)
    axes[0, 0].grid(axis='x', alpha=0.3)
    axes[0, 0].invert_yaxis()
    
    # Plot 2: Position-based attribution
    x_pos = np.arange(len(pos_pivot))
    width = 0.35
    axes[0, 1].bar(x_pos - width/2, pos_pivot['converted'], width, label='Converted', color='#2ecc71', alpha=0.8)
    axes[0, 1].bar(x_pos + width/2, pos_pivot['not_converted'], width, label='Not Converted', color='#e74c3c', alpha=0.8)
    axes[0, 1].set_xlabel('Event Position', fontsize=11, fontweight='bold')
    axes[0, 1].set_ylabel('Avg Attribution', fontsize=11, fontweight='bold')
    axes[0, 1].set_title('**Attribution by Event Position in Sequence**', fontsize=12, fontweight='bold')
    axes[0, 1].set_xticks(x_pos)
    axes[0, 1].set_xticklabels(pos_pivot.index)
    axes[0, 1].legend()
    axes[0, 1].grid(axis='y', alpha=0.3)
    
    # Plot 3: Converted vs Not Converted attention comparison
    top_events_comp = event_pivot.head(15)
    x_comp = np.arange(len(top_events_comp))
    width = 0.35
    
    axes[1, 0].barh(x_comp - width/2, top_events_comp['converted'], width, label='Converted', color='#2ecc71', alpha=0.8)
    axes[1, 0].barh(x_comp + width/2, top_events_comp['not_converted'], width, label='Not Converted', color='#e74c3c', alpha=0.8)
    axes[1, 0].set_yticks(x_comp)
    axes[1, 0].set_yticklabels(top_events_comp.index)
    axes[1, 0].set_xlabel('Avg Attribution', fontsize=11, fontweight='bold')
    axes[1, 0].set_title('**Event Attribution: Converted vs Not Converted**', fontsize=12, fontweight='bold')
    axes[1, 0].legend()
    axes[1, 0].grid(axis='x', alpha=0.3)
    axes[1, 0].invert_yaxis()
    
    # Plot 4: Attribution heatmap by position and event type
    # Get top event types
    top_event_types = event_pivot.head(10).index.tolist()
    heatmap_data = df_grad_attribution[df_grad_attribution['event_type'].isin(top_event_types)]
    heatmap_data = heatmap_data[heatmap_data['event_position'] < 20]  # First 20 events only
    
    pivot_heatmap = heatmap_data.pivot_table(
        index='event_type', columns='event_position', 
        values='gradient_attribution', aggfunc='mean'
    )
    
    sns.heatmap(pivot_heatmap, cmap='YlOrRd', ax=axes[1, 1], cbar_kws={'label': 'Attribution'})
    axes[1, 1].set_xlabel('Event Position', fontsize=11, fontweight='bold')
    axes[1, 1].set_ylabel('Event Type', fontsize=11, fontweight='bold')
    axes[1, 1].set_title('**Attribution Heatmap: Event Type × Position**', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('plots/attention_attribution_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved: plots/attention_attribution_analysis.png")
    
    # ========================================================================
    # Save Results
    # ========================================================================
    df_grad_attribution.to_csv('outputs/gradient_attribution_no_funnel.csv', index=False)
    event_pivot.to_csv('outputs/event_type_attribution_no_funnel.csv')
    pos_pivot.to_csv('outputs/position_attribution_no_funnel.csv')
    
    print("\n✅ ANALYSIS COMPLETE!")
    print("\nResults saved:")
    print("  - outputs/best_transformer_no_funnel.pt (model)")
    print("  - outputs/gradient_attribution_no_funnel.csv (event-level attribution)")
    print("  - outputs/event_type_attribution_no_funnel.csv (event type importance)")
    print("  - outputs/position_attribution_no_funnel.csv (position-based importance)")
    print("  - plots/attention_attribution_analysis.png (visualizations)")
    
    print("\n" + "="*80)
    print("KEY INSIGHTS - Which Events Drive Conversion?")
    print("="*80)
    print("\nTop 5 Most Important Events:")
    for idx, (event_type, row) in enumerate(event_pivot.head(5).iterrows(), 1):
        print(f"{idx}. {event_type:40} | Lift: {row['attribution_lift']:+.4f}")
    
    print("\nMost Important Positions:")
    for pos, row in pos_pivot.iterrows():
        print(f"Position {pos:8} | Lift: {row['attribution_lift']:+.4f}")


if __name__ == "__main__":
    main()

