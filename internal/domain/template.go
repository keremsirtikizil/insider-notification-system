package domain

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Template is a reusable message body with {{variable}} placeholders.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Content   string
	Variables []string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MaxTemplateNameLength bounds template names.
const MaxTemplateNameLength = 100

var (
	placeholderRe  = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)
	templateNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]*$`)
)

// TemplateVariables returns the unique placeholder names in content, in order of
// first appearance.
func TemplateVariables(content string) []string {
	seen := map[string]bool{}
	var vars []string
	for _, m := range placeholderRe.FindAllStringSubmatch(content, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			vars = append(vars, m[1])
		}
	}
	return vars
}

// ValidateTemplateName checks a template name (lowercase slug).
func ValidateTemplateName(name string) error {
	if name == "" {
		return fmt.Errorf("is required")
	}
	if len(name) > MaxTemplateNameLength || !templateNameRe.MatchString(name) {
		return fmt.Errorf("must be a lowercase slug of at most %d characters ([a-z0-9_.-])", MaxTemplateNameLength)
	}
	return nil
}

// RenderTemplate substitutes {{name}} placeholders with values from vars.
// Every placeholder must have a value; unknown extra variables are ignored.
// Values are inserted literally, they are never parsed as template syntax.
func RenderTemplate(content string, vars map[string]string) (string, error) {
	var missing []string
	seen := map[string]bool{}
	for _, name := range TemplateVariables(content) {
		if _, ok := vars[name]; !ok && !seen[name] {
			seen[name] = true
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return "", fmt.Errorf("missing template variables: %s", strings.Join(missing, ", "))
	}
	return placeholderRe.ReplaceAllStringFunc(content, func(match string) string {
		name := placeholderRe.FindStringSubmatch(match)[1]
		return vars[name]
	}), nil
}
