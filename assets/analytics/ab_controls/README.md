# ABControls

Given a baseline conversion rate, minimum detectable effect (MDE), significance level, and statistical power, computes the required per-variant sample size for a proportions z-test. Output is a one-row DataFrame summarizing the calc — surface in the catalog before you run the experiment.

## Example

```yaml
type: dagster_component_templates.ABControlsComponent
attributes:
  asset_name: checkout_v2_sample_size
  upstream_asset_key: any_upstream
  baseline_rate: 0.10
  mde: 0.05
  alpha: 0.05
  power: 0.8
  group_name: experiments
```


## Requirements

```
pandas
scipy
```
