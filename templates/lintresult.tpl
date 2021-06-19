{{ range .LintResults }}
{{ .Topic }} has {{.Severity }}: {{ .Message }} (Hint: {{.Hint}})
{{- end }}
