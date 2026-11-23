package store

import (
	"context"
	"time"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

// QueueDepthRow is the number of unfinished notifications for one
// channel / status / priority combination.
type QueueDepthRow struct {
	Channel  domain.Channel
	Status   domain.Status
	Priority domain.Priority
	Count    int64
}

// QueueDepth counts notifications that are not finished yet.
func (s *Store) QueueDepth(ctx context.Context) ([]QueueDepthRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT channel, status, priority, count(*)
		FROM notifications
		WHERE status IN ('pending', 'scheduled', 'retrying', 'processing')
		GROUP BY channel, status, priority`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueDepthRow
	for rows.Next() {
		var (
			r               QueueDepthRow
			channel, status string
			priority        int16
		)
		if err := rows.Scan(&channel, &status, &priority, &r.Count); err != nil {
			return nil, err
		}
		r.Channel, r.Status, r.Priority = domain.Channel(channel), domain.Status(status), domain.Priority(priority)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ChannelStats summarises one channel over a time window.
type ChannelStats struct {
	Channel domain.Channel
	// Final outcomes reached inside the window.
	Sent   int64
	Failed int64
	// End-to-end latency (created or scheduled time -> provider accepted), ms.
	LatencyAvgMS float64
	LatencyP50MS float64
	LatencyP95MS float64
	LatencyP99MS float64
	// Individual provider calls inside the window.
	Attempts             int64
	AttemptErrors        int64
	ProviderLatencyAvgMS float64
	ProviderLatencyP95MS float64
}

// Stats computes per-channel delivery statistics for the last window.
func (s *Store) Stats(ctx context.Context, window time.Duration) (map[domain.Channel]*ChannelStats, error) {
	out := map[domain.Channel]*ChannelStats{}
	get := func(c domain.Channel) *ChannelStats {
		if out[c] == nil {
			out[c] = &ChannelStats{Channel: c}
		}
		return out[c]
	}

	rows, err := s.pool.Query(ctx, `
		WITH since AS (SELECT now() - make_interval(secs => $1) AS t),
		done AS (
			SELECT n.channel, n.status,
			       EXTRACT(EPOCH FROM (n.sent_at - COALESCE(n.scheduled_at, n.created_at)))::float8 * 1000 AS latency_ms
			FROM notifications n, since
			WHERE n.sent_at >= since.t OR n.failed_at >= since.t
		)
		SELECT channel,
		       count(*) FILTER (WHERE status = 'sent'),
		       count(*) FILTER (WHERE status = 'failed'),
		       COALESCE(avg(latency_ms) FILTER (WHERE status = 'sent'), 0)::float8,
		       COALESCE(percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE status = 'sent'), 0)::float8,
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE status = 'sent'), 0)::float8,
		       COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) FILTER (WHERE status = 'sent'), 0)::float8
		FROM done
		GROUP BY channel`, window.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var channel string
		var r ChannelStats
		if err := rows.Scan(&channel, &r.Sent, &r.Failed, &r.LatencyAvgMS, &r.LatencyP50MS,
			&r.LatencyP95MS, &r.LatencyP99MS); err != nil {
			return nil, err
		}
		cs := get(domain.Channel(channel))
		cs.Sent, cs.Failed = r.Sent, r.Failed
		cs.LatencyAvgMS, cs.LatencyP50MS, cs.LatencyP95MS, cs.LatencyP99MS =
			r.LatencyAvgMS, r.LatencyP50MS, r.LatencyP95MS, r.LatencyP99MS
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.pool.Query(ctx, `
		SELECT channel,
		       count(*),
		       count(*) FILTER (WHERE outcome <> 'success'),
		       COALESCE(avg(duration_ms), 0)::float8,
		       COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms), 0)::float8
		FROM delivery_attempts
		WHERE created_at >= now() - make_interval(secs => $1)
		GROUP BY channel`, window.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var channel string
		var attempts, errorsCount int64
		var avg, p95 float64
		if err := rows.Scan(&channel, &attempts, &errorsCount, &avg, &p95); err != nil {
			return nil, err
		}
		cs := get(domain.Channel(channel))
		cs.Attempts, cs.AttemptErrors, cs.ProviderLatencyAvgMS, cs.ProviderLatencyP95MS = attempts, errorsCount, avg, p95
	}
	return out, rows.Err()
}
