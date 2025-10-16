# Guest-to-Consumer vs Onboarding Analysis with RAU Impact - August 2025

## Executive Summary

This analysis compares the conversion funnel performance of two user cohorts from August 2025, with a focus on understanding the impact of RAU (Retention Action Units / Engagement Milestones):

- **Guest-to-Consumer (iOS + Android)**: Users who signed up from guest checkout (miss onboarding)
- **Onboarding Users (iOS + Android)**: Users who saw onboarding flow

**Key Analysis Dimensions:**
1. Guest-to-Consumer vs Onboarding performance
2. RAU exposure impact within each cohort (Saw RAU vs No RAU)

## Key Questions Answered

### **1. How does missing onboarding affect guest-to-consumer users?**
Compare overall performance between guest-to-consumer and onboarding cohorts

### **2. Does RAU help make up for the lack of onboarding in guest-to-consumer users?**
Within the guest-to-consumer cohort, compare:
- Users who saw RAU vs users who didn't
- Does RAU exposure improve retention and conversion?

### **3. What's the baseline RAU impact in normal onboarding users?**
Within the onboarding cohort, compare RAU vs no RAU performance

## Analysis Structure

### **Cohorts Tracked:**
- Guest-to-Consumer iOS (from `M_REGISTRATION_SUCCESS` where `SOURCE != 'guest' AND IS_GUEST = true`)
- Guest-to-Consumer Android (from `M_SOCIAL_SIGNUP_ICE` where `IS_GUEST_CONSUMER = true`)
- Onboarding Users iOS + Android (from `m_onboarding_start_promo_page_view_ice`)

### **RAU Exposure:**
- Tracked via `M_ENGAGEMENT_MILESTONE_VIEW_ICE`
- Flags created for D1, D7, D14, D28 exposure
- Tracks days to first RAU view and unique milestones seen

### **Conversion Funnel:**
Tracked at D1, D7, D14, D28:
1. App Visit
2. Store Content Page Visit
3. Store Page Visit
4. Add to Cart
5. Cart Page Visit
6. Checkout Page Visit
7. Place Order Action
8. Purchase

### **Success Metrics:**
- **Order Conversion Rate**: % of users who placed orders
- **Orders per User**: Average orders per cohort user
- **First Time Orders**: Orders from new customers (`is_first_ordercart_dd = 1`)
- **MAU**: Count of distinct consumer IDs who placed any order
- **New User MAU**: Count of distinct consumer IDs who placed their first order
- **GMV per User**: Average revenue generated
- **Average Order Value**: GMV / Orders
- **Time to First Order**: Days from cohort date to first order

## Methodology

- **Cohort Period**: August 2025 (2025-08-01 to 2025-08-31)
- **Observation Window**: 28 days post-signup/onboarding
- **Funnel Events**: Tracked via `fact_unique_visitors_full_pt`
- **Order Data**: From `dimension_deliveries` (filtered core orders, non-cancelled)
- **RAU Data**: From `M_ENGAGEMENT_MILESTONE_VIEW_ICE`
- **Platforms**: iOS and Android separately
- **Date**: Analysis run October 2025

## SQL Tables Created

All tables created in `proddb.fionafan` schema:

1. **`guest_vs_onboarding_august_cohorts`** - User cohort definitions with device IDs and consumer IDs
2. **`guest_vs_onboarding_august_funnel_events`** - Daily funnel event tracking (28 days post-cohort)
3. **`guest_vs_onboarding_august_funnel_metrics`** - Aggregated funnel flags by time period (D1/D7/D14/D28)
4. **`guest_vs_onboarding_august_rau`** - RAU exposure flags and timing
5. **`guest_vs_onboarding_august_orders`** - Order counts, first time orders, MAU IDs, GMV data
6. **`guest_vs_onboarding_august_combined`** - Complete user-level dataset with all metrics
7. **`guest_vs_onboarding_august_final_analysis`** - **Final aggregated analysis by cohort, platform, and RAU segment**

## Key Metrics Explained

### **MAU (Monthly Active Users)**
- User-level: Each user has their `consumer_id` in the MAU field if they placed an order, NULL otherwise
- Aggregate: `COUNT(DISTINCT d28_mau) / COUNT(DISTINCT consumer_id)` = % of cohort who became MAU

### **New User MAU**
- User-level: Each user has their `consumer_id` if they placed their **first DoorDash order**, NULL otherwise
- Aggregate: `COUNT(DISTINCT d28_new_user_mau) / COUNT(DISTINCT consumer_id)` = % of cohort who converted as new customers

### **RAU Segment**
- "Saw RAU" = User viewed at least one engagement milestone within 28 days
- "No RAU" = User did not view any engagement milestones within 28 days

## Expected Insights

### **For Guest-to-Consumer Users:**
- How many see RAU despite missing onboarding?
- Does RAU exposure improve their conversion rates?
- Can RAU "make up for" the missed onboarding experience?

### **For Onboarding Users:**
- Baseline RAU exposure rate
- RAU impact on already-onboarded users
- Comparison benchmark for guest-to-consumer RAU impact

---

*Analysis Date: October 10, 2025*
*Cohort: August 2025 Signups*


