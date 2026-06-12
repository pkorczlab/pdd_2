# Results note

## Run metadata

- Run date: 2026-06-12
- Runtime from logs: about 85.7 minutes on agents and about 85.9 minutes on the central node.
- Agents: `agent-us`, `agent-pl`, `agent-jp`, `agent-au`
- Central node: `central-pl`
- Sampling period: 20 seconds
- STUN timeout: 500 ms
- SLA threshold: 200 ms
- STUN servers: 10 servers from the assignment list.

The run satisfies the requirement to run the system for at least one hour. The collected log span is:

- `agent-us`: 2026-06-12T09:05:19Z to 2026-06-12T10:31:02Z
- `agent-pl`: 2026-06-12T09:05:16Z to 2026-06-12T10:30:59Z
- `agent-jp`: 2026-06-12T09:05:23Z to 2026-06-12T10:31:05Z
- `agent-au`: 2026-06-12T09:05:24Z to 2026-06-12T10:31:08Z
- `central-pl`: 2026-06-12T09:05:36Z to 2026-06-12T10:31:29Z

## Log validation

Each agent collected 258 measurements for each of the 10 STUN servers, so each agent wrote 2580 `stun_metrics` records. The derived topics were also produced uniformly for all servers:

- `stun_availability`: 257 records per server on every agent.
- `stun_avg_latency`: 257 records per server on every agent.
- `stun_avg_availability`: 257 records per server on every agent.
- `stun_bottom_ten_percent_latency`: 248 records per server on every agent.

The one-record difference between `stun_metrics` and the first three derived topics means that the last 20-second batch was probably still being processed or not yet flushed when logs were fetched. This is about 0.4% of data, below the allowed 5% data-loss limit. The smaller count for `stun_bottom_ten_percent_latency` is expected, because the reservoir sample must first collect enough values before it can return the 10th highest value.

The central node wrote 10320 `stun_global_availability` records, which corresponds to 4 agents x 258 cycles x 10 servers. Plot logs contain one plotted value roughly every 72 seconds, which is about 2% of a one-hour run:

- 65 points per server for `stun_avg_latency` on every agent.
- 65 points per server for `stun_avg_availability` on every agent.
- 62-63 points per server for `stun_bottom_ten_percent_latency` on every agent.
- 65 points per server for `stun_global_availability` on the central node.

No malformed JSON lines were found in the fetched logs.

## Attachments

Generated plots are in `plots/`:

- `agent-us_stun_avg_latency.png`
- `agent-us_stun_avg_availability.png`
- `agent-us_stun_bottom_ten_percent_latency.png`
- `agent-pl_stun_avg_latency.png`
- `agent-pl_stun_avg_availability.png`
- `agent-pl_stun_bottom_ten_percent_latency.png`
- `agent-jp_stun_avg_latency.png`
- `agent-jp_stun_avg_availability.png`
- `agent-jp_stun_bottom_ten_percent_latency.png`
- `agent-au_stun_avg_latency.png`
- `agent-au_stun_avg_availability.png`
- `agent-au_stun_bottom_ten_percent_latency.png`
- `central-pl_stun_global_availability.png`

Representative logs are in `run_logs/`. The log files include full topic values with timestamps.

## Latency observations

Latency depends strongly on the agent location. The Polish agent had very low latency to European STUN servers: `stun.schlund.de`, `stun.voipbuster.com`, `stun.sipgate.net` and `global.stun.twilio.com` were mostly around 28-46 ms on average. The US agent had low latency to US-oriented servers: `stun.sip.us`, `global.stun.twilio.com`, `stun.freeswitch.org` and `stun.voip.blackberry.com` were mostly around 39-63 ms on average. The Australia and Japan agents had very low latency to `stun.l.google.com` and `global.stun.twilio.com`, but many European or unstable servers were above the 200 ms SLA threshold.

`stun.l.google.com:19302` was the most stable and fastest server from all locations. It had average latency around 3-4 ms and 100% SLA availability on most agents. `global.stun.twilio.com:3478` was also stable, with high availability and low latency from all regions.

Some servers were reachable but often too slow for the 200 ms SLA. For example, from Australia, `stun.schlund.de`, `stun.voipbuster.com`, `stun.counterpath.com` and `stun.ekiga.net` had average latencies around 296-360 ms and therefore failed the SLA even when they answered. This shows that availability in this assignment is not just whether a server responds, but whether it responds within 200 ms.

## Availability observations

The final central `stun_global_availability` values were:

- `stun.l.google.com:19302`: 1.0000
- `global.stun.twilio.com:3478`: 0.9833
- `stun.freeswitch.org:3478`: 0.8208
- `stun.sip.us:3478`: 0.7250
- `stun.sipgate.net:10000`: 0.6583
- `stun.voip.blackberry.com:3478`: 0.5167
- `stun.voipbuster.com:3478`: 0.4792
- `stun.schlund.de:3478`: 0.4750
- `stun.counterpath.com:3478`: 0.3917
- `stun.ekiga.net:3478`: 0.3542

The best global SLA results came from `stun.l.google.com` and `global.stun.twilio.com`. The weakest global results were `stun.ekiga.net` and `stun.counterpath.com`, mostly because of frequent 500 ms timeouts and many responses above 200 ms.

Regional differences were visible. `stun.sipgate.net:10000` worked well from Poland and the US, but from Japan it timed out for the whole run. `stun.counterpath.com` and `stun.ekiga.net` were unstable in all regions, with many 500 ms timeout values. Australia had especially poor SLA availability for several non-local servers, even when they technically responded.

## Notes on algorithms

`stun_avg_latency` uses an exact sliding window over the last 10 minutes. `stun_avg_availability` uses DGIM over the last 20 minutes and counts zeros internally, as required, because SLA failures are expected to be less frequent than successes. `stun_bottom_ten_percent_latency` uses reservoir sampling with size 100 and a heap to compute the 10th highest sample value without sorting the whole sample each time. The central node computes `stun_global_availability` as an exact sliding-window average over `stun_availability` records consumed from all four agent brokers.

## Unexpected results

The most surprising result was the full timeout of `stun.sipgate.net:10000` from Japan while the same server worked well from Poland and reasonably from the US and Australia. Another unexpected result was that some servers had 100% response rate but still very poor SLA availability because the latency was consistently above 200 ms. This happened especially for Australia to several European servers.
