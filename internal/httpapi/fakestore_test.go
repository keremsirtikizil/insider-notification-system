package httpapi

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
	"github.com/keremsirtikizil/insider-notification-system/internal/store"
)

// fakeStore is an in-memory Store for handler tests. The SQL implementation is
// covered by the integration tests in internal/store.
type fakeStore struct {
	mu        sync.Mutex
	clock     time.Time
	rows      map[uuid.UUID]*domain.Notification
	batches   map[uuid.UUID]*domain.Batch
	templates map[uuid.UUID]*domain.Template
	err       error // forced error for every call when set
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		clock: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		rows:  map[uuid.UUID]*domain.Notification{}, batches: map[uuid.UUID]*domain.Batch{},
		templates: map[uuid.UUID]*domain.Template{},
	}
}

func (f *fakeStore) tick() time.Time {
	f.clock = f.clock.Add(time.Millisecond)
	return f.clock
}

func (f *fakeStore) CreateNotifications(_ context.Context, batch *domain.Batch, ns []*domain.Notification) ([]store.CreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	results := make([]store.CreateResult, len(ns))
	for i, n := range ns {
		if n.IdempotencyKey != nil {
			for _, e := range f.rows {
				if e.IdempotencyKey != nil && *e.IdempotencyKey == *n.IdempotencyKey {
					if !e.SamePayload(n) {
						return nil, &store.IdempotencyConflictError{Index: i, Key: *n.IdempotencyKey}
					}
					cp := *e
					results[i] = store.CreateResult{Notification: &cp}
				}
			}
			if results[i].Notification != nil {
				continue
			}
		}
		cp := *n
		cp.CreatedAt = f.tick()
		cp.UpdatedAt = cp.CreatedAt
		cp.NextAttemptAt = cp.CreatedAt
		if cp.ScheduledAt != nil {
			cp.NextAttemptAt = *cp.ScheduledAt
		}
		f.rows[cp.ID] = &cp
		out := cp
		results[i] = store.CreateResult{Notification: &out, Created: true}
	}
	if batch != nil {
		created := 0
		for _, r := range results {
			if r.Created {
				created++
			}
		}
		batch.Total, batch.CreatedAt = created, f.tick()
		f.batches[batch.ID] = batch
	}
	return results, nil
}

func (f *fakeStore) GetNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	n, ok := f.rows[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *n
	return &cp, nil
}

func (f *fakeStore) ListNotifications(_ context.Context, flt store.ListFilter) ([]*domain.Notification, *store.Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var items []*domain.Notification
	for _, n := range f.rows {
		switch {
		case flt.Status != nil && n.Status != *flt.Status,
			flt.Channel != nil && n.Channel != *flt.Channel,
			flt.Priority != nil && n.Priority != *flt.Priority,
			flt.BatchID != nil && (n.BatchID == nil || *n.BatchID != *flt.BatchID),
			flt.CreatedFrom != nil && n.CreatedAt.Before(*flt.CreatedFrom),
			flt.CreatedTo != nil && !n.CreatedAt.Before(*flt.CreatedTo):
			continue
		}
		if flt.After != nil && !n.CreatedAt.Before(flt.After.CreatedAt) {
			continue
		}
		cp := *n
		items = append(items, &cp)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	if len(items) <= flt.Limit {
		return items, nil, nil
	}
	items = items[:flt.Limit]
	last := items[len(items)-1]
	return items, &store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID}, nil
}

func (f *fakeStore) transition(id uuid.UUID, allowed func(domain.Status) bool, apply func(*domain.Notification)) (*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.rows[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if !allowed(n.Status) {
		return nil, fmt.Errorf("%w: notification is %s", store.ErrInvalidState, n.Status)
	}
	apply(n)
	n.UpdatedAt = f.tick()
	cp := *n
	return &cp, nil
}

func (f *fakeStore) CancelNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return f.transition(id, domain.Status.Cancellable, func(n *domain.Notification) {
		now := f.clock
		n.Status, n.CancelledAt = domain.StatusCancelled, &now
	})
}

func (f *fakeStore) RetryNotification(_ context.Context, id uuid.UUID) (*domain.Notification, error) {
	return f.transition(id, func(s domain.Status) bool { return s == domain.StatusFailed }, func(n *domain.Notification) {
		n.Status, n.Attempts, n.FailedAt = domain.StatusPending, 0, nil
	})
}

func (f *fakeStore) ListAttempts(context.Context, uuid.UUID) ([]domain.DeliveryAttempt, error) {
	return []domain.DeliveryAttempt{}, nil
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID) (*domain.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.batches[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	out := &domain.Batch{ID: b.ID, Total: b.Total, CreatedAt: b.CreatedAt, StatusCounts: map[domain.Status]int{}}
	for _, n := range f.rows {
		if n.BatchID != nil && *n.BatchID == id {
			out.StatusCounts[n.Status]++
		}
	}
	return out, nil
}

func (f *fakeStore) CancelBatch(_ context.Context, id uuid.UUID) ([]*domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.batches[id]; !ok {
		return nil, store.ErrNotFound
	}
	var out []*domain.Notification
	for _, n := range f.rows {
		if n.BatchID != nil && *n.BatchID == id && n.Status.Cancellable() {
			n.Status = domain.StatusCancelled
			cp := *n
			out = append(out, &cp)
		}
	}
	return out, nil
}

func (f *fakeStore) CreateTemplate(_ context.Context, t *domain.Template) (*domain.Template, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.templates {
		if e.Name == t.Name {
			return nil, store.ErrDuplicate
		}
	}
	cp := *t
	cp.CreatedAt, cp.UpdatedAt = f.tick(), f.clock
	f.templates[cp.ID] = &cp
	return &cp, nil
}

func (f *fakeStore) GetTemplate(_ context.Context, id uuid.UUID) (*domain.Template, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.templates[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return t, nil
}

func (f *fakeStore) ListTemplates(context.Context) ([]*domain.Template, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := []*domain.Template{}
	for _, t := range f.templates {
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeStore) QueueDepth(context.Context) ([]store.QueueDepthRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []store.QueueDepthRow
	for _, n := range f.rows {
		if n.Status.Cancellable() || n.Status == domain.StatusProcessing {
			out = append(out, store.QueueDepthRow{Channel: n.Channel, Status: n.Status, Priority: n.Priority, Count: 1})
		}
	}
	return out, nil
}

func (f *fakeStore) Stats(context.Context, time.Duration) (map[domain.Channel]*store.ChannelStats, error) {
	return map[domain.Channel]*store.ChannelStats{
		domain.ChannelSMS: {Channel: domain.ChannelSMS, Sent: 90, Failed: 10, LatencyP95MS: 120, Attempts: 110, AttemptErrors: 20},
	}, nil
}
