package store

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/keremsirtikizil/insider-notification-system/internal/domain"
)

const templateColumns = `id, name, channel, content, variables, created_at, updated_at`

func scanTemplate(row pgx.Row) (*domain.Template, error) {
	var (
		t       domain.Template
		channel string
	)
	if err := row.Scan(&t.ID, &t.Name, &channel, &t.Content, &t.Variables, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	t.Channel = domain.Channel(channel)
	if t.Variables == nil {
		t.Variables = []string{}
	}
	return &t, nil
}

// CreateTemplate stores a template. Names are unique.
func (s *Store) CreateTemplate(ctx context.Context, t *domain.Template) (*domain.Template, error) {
	vars := t.Variables
	if vars == nil {
		vars = []string{}
	}
	created, err := scanTemplate(s.pool.QueryRow(ctx, `
		INSERT INTO templates (id, name, channel, content, variables)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING `+templateColumns,
		t.ID, t.Name, string(t.Channel), t.Content, vars))
	if isUniqueViolation(err) {
		return nil, ErrDuplicate
	}
	return created, err
}

// GetTemplate returns one template.
func (s *Store) GetTemplate(ctx context.Context, id uuid.UUID) (*domain.Template, error) {
	t, err := scanTemplate(s.pool.QueryRow(ctx,
		`SELECT `+templateColumns+` FROM templates WHERE id = $1`, id))
	return t, notFoundIfNoRows(err)
}

// ListTemplates returns all templates ordered by name.
func (s *Store) ListTemplates(ctx context.Context) ([]*domain.Template, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+templateColumns+` FROM templates ORDER BY name LIMIT 1000`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []*domain.Template{}
	for rows.Next() {
		t, err := scanTemplate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}
