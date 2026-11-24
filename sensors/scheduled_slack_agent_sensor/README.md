# Scheduled Slack Agent Sensor

A scheduled agent run that announces itself in Slack.

Every `period_hours` the sensor posts `announce_text` to a channel, then starts the agent run tagged
`slack/channel`, `slack/thread_ts` and `slack/message_ts`. The `thread_ts` is the ts of the message it
just posted, so a downstream `slack_reply` asset answers in that thread.

Use `cron_schedule` instead when nothing needs to be posted. Use `slack_agent_request_sensor` when a
person's message should start the run.

## Example

```yaml
type: dagster_community_components.ScheduledSlackAgentSensorComponent
attributes:
  sensor_name: weekly_improvement_review
  slack_channel_id: "C0123ABCDEF"
  asset_selection: [review_agent, review_reply]
  announce_text: ":bar_chart: Improvement review started"
  period_hours: 168
```

## Timing

- The period starts at the first tick, not at zero. The first run is `first_run_delay_hours` later,
  which defaults to one full period, so activating the sensor or deploying never fires a run at once.
  Set `first_run_delay_hours: 0` to fire on the first tick.
- A tick inside the period skips and reports the time left (for example "next run in 2d 3h").
- The next due time is stored in the cursor, so changing `period_hours` later does not re-fire a run
  that already happened.

## Failure behaviour

The post comes first, because the run needs its ts. If Slack rejects it (bad channel, bot not invited,
token missing) no run starts, the cursor does not move, and the next tick retries. The run key is
derived from the posted ts, so one announcement never starts two runs.

## Requirements

A bot token in `SLACK_BOT_TOKEN` (or `slack_bot_token_env_var`) with `chat:write`, invited to the channel.
Tick-skipping inside Dagster+ branch deployments is on by default.
