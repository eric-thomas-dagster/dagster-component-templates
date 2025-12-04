# CDP Pipeline Templates

Pre-built, production-ready CDP pipelines for common use cases. Each pipeline chains multiple components together to solve a specific business problem.

## Philosophy: Use-Case Driven Development

Instead of starting with data ingestion and working forward, these pipelines start with the **business outcome** and work backwards. This is how modern CDP platforms work - you pick a use case, and the platform configures the entire pipeline for you.

## Available Pipelines

### Customer Lifetime Value & Segmentation
**File**: `customer_ltv_pipeline.yaml`
**Use Case**: Identify high-value customers and optimize acquisition spend
**Flow**: Shopify Orders → LTV Prediction → Value-Based Segmentation → Marketing Activation

### Marketing Attribution & ROI
**File**: `marketing_attribution_pipeline.yaml`
**Use Case**: Understand which marketing channels drive conversions
**Flow**: Marketing Touchpoints → Attribution → Campaign Performance → Budget Optimization

### Churn Prevention
**File**: `churn_prevention_pipeline.yaml`
**Use Case**: Identify and save at-risk customers before they churn
**Flow**: Multiple Sources → Customer 360 → Churn Prediction → High-Risk Alerts

### Product Recommendations Engine
**File**: `product_recommendations_pipeline.yaml`
**Use Case**: Power product recommendations across channels
**Flow**: Order Items → Collaborative Filtering → Recommendation API → Email/Web Integration

### Customer Journey Optimization
**File**: `customer_journey_pipeline.yaml`
**Use Case**: Optimize conversion funnels and reduce drop-offs
**Flow**: Event Stream → Journey Mapping → Funnel Analysis → Conversion Optimization

### Experimentation Platform
**File**: `ab_testing_pipeline.yaml`
**Use Case**: Data-driven product decisions through rigorous testing
**Flow**: Experiment Events → A/B Test Analysis → Statistical Validation → Decision Support

### Propensity-Based Marketing
**File**: `propensity_marketing_pipeline.yaml`
**Use Case**: Target customers with highest propensity to convert
**Flow**: Behavior Data → Propensity Scoring → Segment Creation → Campaign Targeting

### Fraud Detection
**File**: `fraud_detection_pipeline.yaml`
**Use Case**: Detect and prevent fraudulent transactions in real-time
**Flow**: Transactions → Anomaly Detection → Risk Scoring → Alert System

### Complete Customer Data Platform
**File**: `full_cdp_pipeline.yaml`
**Use Case**: End-to-end CDP with all analytics capabilities
**Flow**: Multi-Source Ingestion → Standardization → Complete Analytics Suite → Activation

## How to Use These Pipelines

### Option 1: Dagster Designer (Visual)
1. Open Dagster Designer UI
2. Import pipeline YAML file
3. Adjust component parameters for your data
4. Deploy to your Dagster instance

### Option 2: Code-First (CLI)
```bash
# Copy pipeline to your project
cp pipelines/customer_ltv_pipeline.yaml my-dagster-project/components/

# Deploy with dg
cd my-dagster-project
dg deploy

# Run on schedule
dg run customer_ltv_pipeline
```

### Option 3: Customize & Extend
1. Start with a template pipeline
2. Add/remove components based on your needs
3. Connect to your specific data sources
4. Add custom transformations

## Pipeline Architecture Patterns

### Pattern 1: Linear Pipeline
```
Source → Standardizer → Analytics → Activation
```
Simple, easy to understand, good for single-purpose pipelines.

### Pattern 2: Hub-and-Spoke
```
        → Analytics A →
Source →  → Analytics B  → Unified Output
        → Analytics C →
```
Multiple analytics on same standardized data.

### Pattern 3: Multi-Source Merge
```
Source A →             → Analytics →
Source B → Standardizer →          → Unified View
Source C →             →           →
```
Combine data from multiple sources.

### Pattern 4: Feedback Loop
```
Source → Analytics → Activation
           ↓            ↑
         Model Training (improves over time)
```
Continuous improvement with feedback.

## Scheduling & Orchestration

Each pipeline includes:
- **Schedules**: Automatic execution (daily, hourly, etc.)
- **Sensors**: Event-driven triggers
- **Asset Dependencies**: Automatic dependency resolution
- **Partitioning**: Time-based or key-based partitioning
- **Retry Policies**: Automatic retries on failure

## Competitive Advantages vs. Traditional CDPs

| Feature | Traditional CDP | Dagster CDP Pipelines |
|---------|----------------|----------------------|
| **Customization** | Limited, proprietary | Fully customizable, open source |
| **Data Warehouse** | Separate, export required | Native warehouse integration |
| **Cost** | $$$$ per MTU | Infrastructure cost only |
| **Extensibility** | Locked-in ecosystem | Add any Python library |
| **Data Governance** | Black box | Full visibility & control |
| **Deployment** | SaaS only | Deploy anywhere (cloud, on-prem) |
| **Real-time** | Extra cost | Native support |
| **ML Integration** | Limited | Full Python ML ecosystem |

## Example: From Segment to Dagster

**Segment CDP approach:**
1. Sign up for Segment ($$$)
2. Implement Segment SDK in your app
3. Configure destinations in UI
4. Pay per MTU (Monthly Tracked Users)
5. Limited to Segment's integrations

**Dagster CDP approach:**
1. Copy `full_cdp_pipeline.yaml`
2. Point to your data sources
3. Customize analytics for your business
4. Deploy to your infrastructure
5. Pay only for compute (10x+ cheaper)
6. Full control and customization

## Next Steps

1. **Browse pipelines** in this directory
2. **Pick a use case** that matches your needs
3. **Copy the template** to your project
4. **Customize** for your data sources
5. **Deploy & iterate**

## Developing New Pipelines

Want to create your own pipeline template? See our comprehensive guide:

**📚 [Pipeline Development Guide](./PIPELINE_DEVELOPMENT.md)**

Covers:
- YAML formats and structure
- Parameter configuration
- Component dependencies
- Conditional logic
- Environment-specific configuration
- Testing and publishing
- Complete examples

## Contributing

Have a great pipeline template? Submit a PR! We're building the world's best open-source CDP.

See [PIPELINE_DEVELOPMENT.md](./PIPELINE_DEVELOPMENT.md) for detailed contribution guidelines.
