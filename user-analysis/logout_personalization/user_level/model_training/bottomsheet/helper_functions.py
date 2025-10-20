import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
import pandas as pd
from IPython.display import display
import numpy as np
from sklift.metrics import qini_auc_score, uplift_at_k

# display confusion matrix
def display_confusion_matrix(y_true, y_pred):
    cm = confusion_matrix(y_true, y_pred)
    labels = ['Did Not Convert', 'Converted'] 
    plt.figure(figsize=(5, 3))
    sns.heatmap(cm, annot=True, fmt='d', linewidths=.5, cbar=False, xticklabels=labels, yticklabels=labels)
    plt.title('Confusion Matrix')
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.show()  


def uplift_metrics(model_name, preds, y_true, treatment):

    results = []

    # Qini coefficient
    qini = qini_auc_score(y_true, preds, treatment)

    # AUUC (approx via area under uplift curve)
    sorted_idx = np.argsort(-preds)
    y_sorted, t_sorted = np.array(y_true)[sorted_idx], np.array(treatment)[sorted_idx]

    treated = (t_sorted == 1)
    control = (t_sorted == 0)
    cum_treat = np.cumsum(y_sorted * treated) / max(1, treated.sum())
    cum_control = np.cumsum(y_sorted * control) / max(1, control.sum())
    uplift_curve = cum_treat - cum_control
    auuc = np.trapz(uplift_curve) / len(y_sorted)

    # Uplift at top-30%
    uplift_top_30 = uplift_at_k(y_true, preds, treatment, strategy="by_group", k=0.3)
    uplift_top_50 = uplift_at_k(y_true, preds, treatment, strategy="by_group", k=0.5)
    uplift_top_70 = uplift_at_k(y_true, preds, treatment, strategy="by_group", k=0.8)

    results.append({
        "Model": model_name,
        "Qini": qini,
        "AUUC": auuc,
        "Uplift@30%": uplift_top_30,
        "Uplift@50%": uplift_top_50,
        "Uplift@70%": uplift_top_70
    })

    results_df = pd.DataFrame(results)
    display(results_df)

def plot_qini_curve(uplift, title, y_true, treatment):
    """
    Plots Qini curve: model vs random baseline.
    uplift: predicted uplift scores
    y_true: actual outcomes
    t: treatment indicator
    """
    # Sort users by predicted uplift (descending)
    order = np.argsort(-uplift)
    y_sorted = y_true[order]
    t_sorted = treatment[order]

    # Cumulative sums
    cum_y_t = np.cumsum(y_sorted * t_sorted)
    cum_y_c = np.cumsum(y_sorted * (1 - t_sorted))

    # Incremental effect at each step
    inc = cum_y_t - cum_y_c
    frac = np.arange(1, len(uplift) + 1) / len(uplift)

    # Random baseline = straight line from (0,0) to (1, total incremental effect)
    total_inc = inc[-1]
    baseline = frac * total_inc

    # Plot
    plt.figure(figsize=(6, 4))
    plt.plot(frac, inc, label="Model Qini curve")
    plt.plot(frac, baseline, "--", color="gray", label="Random baseline")
    plt.title(title)
    plt.xlabel("Fraction of population targeted")
    plt.ylabel("Cumulative incremental conversions")
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.legend()
    plt.show()

def qini_curve(y_true, treatment, uplift_score):
    """
    Compute Qini curve data.
    y_true: outcomes (0/1)
    treatment: treatment assignment (0/1)
    uplift_score: predicted uplift score (p_treat - p_control)
    """
    df = pd.DataFrame({
        "y": y_true,
        "t": treatment,
        "uplift": uplift_score
    }).sort_values("uplift", ascending=False).reset_index(drop=True)

    # cumulative treatment & control counts
    df["n_treat"] = (df.t == 1).cumsum()
    df["n_control"] = (df.t == 0).cumsum()
    
    # cumulative outcome sums
    df["y_treat"] = ((df.t == 1) & (df.y == 1)).cumsum()
    df["y_control"] = ((df.t == 0) & (df.y == 1)).cumsum()

    # expected control outcomes if they had same size as treatment
    df["y_control_adj"] = df["y_control"] * (df["n_treat"] / df["n_control"].replace(0, np.nan))

    # uplift curve
    df["uplift_curve"] = df["y_treat"] - df["y_control_adj"].fillna(0)

    plt.plot(df.index / len(df), df["uplift_curve"], label="Uplift curve")
    plt.xlabel("Proportion of population targeted")
    plt.ylabel("Incremental outcomes")
    plt.title("Qini Curve")
    plt.legend()
    plt.show()

    return df


def qini_coefficient(df):
    """
    Compute Qini coefficient = area under Qini curve
    """
    return np.trapezoid(df["uplift_curve"], dx=1) / len(df)


def uplift_at_k(y_true, treatment, uplift_score, k=0.3):
    """
    Uplift at top k fraction of population
    """
    df = pd.DataFrame({
        "y": y_true,
        "t": treatment,
        "uplift": uplift_score
    }).sort_values("uplift", ascending=False).reset_index(drop=True)

    top_k = int(len(df) * k)
    df_top = df.iloc[:top_k]

    treat_rate = df_top[df_top.t == 1].y.mean()
    control_rate = df_top[df_top.t == 0].y.mean()

    return treat_rate - control_rate