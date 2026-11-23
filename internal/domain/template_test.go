package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTemplateVariables(t *testing.T) {
	vars := TemplateVariables("Hi {{name}}, your code is {{ code }}. Bye {{name}}! {{ not valid-name }}")
	assert.Equal(t, []string{"name", "code"}, vars)
	assert.Empty(t, TemplateVariables("no placeholders"))
}

func TestRenderTemplate(t *testing.T) {
	out, err := RenderTemplate("Hi {{name}}, {{ discount }}% off today", map[string]string{
		"name": "Ayşe", "discount": "20", "unused": "x",
	})
	require.NoError(t, err)
	assert.Equal(t, "Hi Ayşe, 20% off today", out)
}

func TestRenderTemplateMissingVariables(t *testing.T) {
	_, err := RenderTemplate("{{b}} {{a}} {{b}}", map[string]string{})
	require.Error(t, err)
	assert.Equal(t, "missing template variables: a, b", err.Error())
}

func TestRenderTemplateDoesNotExpandValues(t *testing.T) {
	// A value that looks like a placeholder must be inserted literally.
	out, err := RenderTemplate("Hello {{name}}", map[string]string{"name": "{{secret}}"})
	require.NoError(t, err)
	assert.Equal(t, "Hello {{secret}}", out)
}

func TestValidateTemplateName(t *testing.T) {
	assert.NoError(t, ValidateTemplateName("order.shipped_v2"))
	assert.Error(t, ValidateTemplateName(""))
	assert.Error(t, ValidateTemplateName("Has Spaces"))
	assert.Error(t, ValidateTemplateName("-leading-dash"))
}
