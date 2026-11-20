export type DispatchSummary = {
  source_events: number
  expected_pairs: number
  complete_pairs: number
  missing_pairs: number
  unclaimed_pairs: number
  late_receipts: number
  duplicate_receipts: number
  claims: number
  actual_replays: number
}

export type CoverageRow = {
  dispatch_id: string
  event_id: string
  source_db: string
  source_table: string
  operation: string
  tx_id: string
  binlog_file: string
  binlog_pos: number
  gtid: string
  event_time: string
  target: string
  rule_name: string
  rule_version: number
  frozen_at: string
  received: boolean
  coverage_status: string
  receipt_id: string | null
  receipt_kind: string | null
  received_at: string | null
  receipt_channel: string | null
  replay_count: number
  claim_id: string | null
  claim_state: string | null
  before_image: string
  after_image: string
}

export type TargetSummary = {
  target: string
  expected: number
  complete: number
  missing: number
  replays: number
}

export type ClaimPair = {
  event_id: string
  target: string
  state: string
  replay_count: number
  replayed_at: string | null
  closed_by_receipt: string | null
  closed_at: string | null
}

export type ClaimRecord = {
  claim_id: string
  operator: string
  claimed_at: string
  total: number
  pending: number
  replayed: number
  closed_by_late_receipt: number
  actual_replays: number
  pairs: ClaimPair[]
}

export type ReceiptRecord = {
  receipt_id: string
  event_id: string
  target: string
  kind: string
  received_at: string
  applied: boolean
  counted_as_complete: boolean
  dedup_reason: string
  channel: string
}

export type SourceEvent = {
  event_id: string
  source_db: string
  source_table: string
  operation: string
  tx_id: string
  event_time: string
  binlog_file: string
  binlog_pos: number
  gtid: string
  before_image: string
  after_image: string
  replayable: boolean
  frozen_targets: string[]
  expected_pairs: number
  complete_pairs: number
}

export type ConfigDrift = {
  event_id: string
  rule_name: string
  rule_version: number
  current_targets: string[]
  frozen_targets: string[]
  changed: boolean
  note: string
}

export type DispatchBoard = {
  summary: DispatchSummary
  targets: TargetSummary[]
  coverage: CoverageRow[]
  missing: CoverageRow[]
  claims: ClaimRecord[]
  receipts: ReceiptRecord[]
  resends: { resend_id: string; claim_id: string; event_id: string; target: string }[]
  source_events: SourceEvent[]
  config_drift: ConfigDrift[]
}
