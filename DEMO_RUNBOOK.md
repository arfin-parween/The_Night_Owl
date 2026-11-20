# 🎤 Stage runbook: "Building an Autonomous AI Engineer with OpenClaw and OpenSearch"

60 minutes · OpenSearch Bengaluru Meetup · 21 Nov

## Screen layout
- **Left:** terminal with 2 tabs: `watch` (tab 1) and `presenter` (tab 2). Font size 18+.
- **Right:** Telegram Desktop (or phone mirrored with scrcpy) showing the bot chat.
- **Spare tab:** OpenSearch Dashboards → Discover on `nightowl-logs`.

## 30 minutes before doors open
```bash
cd ~/nightowl && source .venv/bin/activate
docker compose up -d && nightowl doctor      # everything ✅
nightowl reset                               # green baseline
openclaw gateway status
```
Send `/new` to the bot so the session is fresh. Turn on Do Not Disturb everywhere except Telegram.
Hotspot as backup Wi-Fi (the LLM call needs internet).

## Session flow

| Time | Segment | What happens |
|---|---|---|
| 0–4 | **Cold open (live)** | Say: "Before I introduce myself, let me break production." Run `nightowl break schema_drift --run`. Phone buzzes 🚨. "While I talk, an AI engineer is already investigating. We'll come back to it." |
| 4–10 | The 2 AM problem | Why on-call is painful: alert → 40 min of grep across 5 tools. Agentic AI = LLM + tools + loop + memory + guardrails. Not a chatbot. |
| 10–13 | **Back to Telegram** | The RCA has arrived. Read it out: root cause, evidence, ruled-out change, past incident, SLA minutes. Don't approve yet. |
| 13–22 | Architecture | Diagram from the README. OpenClaw: Gateway, skills, exec, hooks, channels. Why one CLI tool and not raw shell. |
| 22–32 | OpenSearch as the AI's memory and eyes | Index design (logs/runs/changes/incidents/lineage). Show `nightowl schema-diff latest` + `changes` in the terminal. ML Commons model inside the cluster, ingest pipeline, knn_vector, hybrid query + normalization pipeline. Show `similar` hitting INC-0412 even though it's a different pipeline and different words. |
| 32–36 | **Approve live** | Reply `approve INC-xxxx` on Telegram. Config diff → Spark re-run → ✅ → MTTR ~3–5 min vs 90 min for INC-0412. |
| 36–46 | **Audience picks the failure** | Run `nightowl reset`, then let the audience choose `bad_data` or `missing_partition`. This shows it isn't scripted. For `missing_partition`, the agent should *refuse to rerun* until `nightowl deliver`. Then approve. |
| 46–50 | **It learned** | `nightowl reset && nightowl break schema_drift --run` again. The agent now cites the incident it resolved 30 minutes ago. |
| 50–53 | Guardrails + honesty | Approval protocol, fix whitelist, audit index, untrusted logs. Optional: `break bad_data --with-injection --run` (only if it passed in rehearsal). Limits: wrong diagnoses, cost, exec allowlist is the real boundary. |
| 53–60 | Takeaways + Q&A | "Search is the foundation of reliable agents." Repo link QR. |

## Exact commands
```bash
# tab 1 (leave running the whole talk)
nightowl watch

# tab 2
nightowl break schema_drift --run
nightowl reset
nightowl break bad_data --run
nightowl reset
nightowl break missing_partition --run
nightowl deliver            # after the agent says it's waiting for upstream
nightowl reset
nightowl break bad_data --with-injection --run   # optional
```
Expected agent fixes: schema_drift → `add_column_alias order_value_inr → order_amount`;
bad_data → `enable_quarantine` (15k rows from store-app 7.2.0); missing_partition → `rerun_only`
only after the partition lands (it should also spot the DB replica lag + VACUUM FULL).

## If something breaks on stage
| Problem | Do this |
|---|---|
| Phone doesn't buzz | Check tab 1. If the hook failed, type in Telegram: *"daily_city_sales just failed, investigate"*. Same skill, same result. |
| LLM slow (>2 min) | Keep talking architecture, then come back. Have the **backup recording** ready. |
| Agent gets it wrong | Great teaching moment: "This is why we have human approval." Run the tools yourself: `nightowl timeline latest`, `schema-diff`, `changes`. |
| OpenSearch down | `docker compose restart opensearch`, wait 60s, `nightowl doctor`. |
| Internet dies | Play the backup recording. Then do the tool-by-tool walkthrough in the terminal (works offline). |

**Record a full backup video** of the cold open → RCA → approve → green flow tonight (OBS or Xbox Game Bar).

## Likely questions from senior engineers
- **"How do you stop hallucinated root causes?"** Every claim has to cite tool output (run ids, values,
  timestamps). It must name a ruled-out change. It can't act without approval, and the fix space is a whitelist.
- **"Why OpenSearch and not a vector DB?"** The evidence is logs, aggregations, time ranges *and* semantic
  memory. That's one engine and one query (hybrid), and the embedding model runs inside the cluster with no extra service.
- **"Why not just rules?"** Rules handle known failures. The agent correlates across systems it was never
  explicitly wired to (a DB VACUUM causing a missing partition) and explains in English. Rules can still do detection (the watcher is one).
- **"Cost / latency?"** About 6–9 tool calls per incident, a few cents, 1–2 min, compared with ~90 min MTTR historically.
- **"Security?"** The CLI guard is a speed bump. The real boundary is the exec allowlist plus OS isolation plus read-only creds,
  with write access scoped to config.
- **"How would this run in prod?"** OpenSearch Alerting monitor → webhook instead of the poller; per-team skills;
  evals on replayed incidents; PRs instead of direct config edits.
