------
{{ range .Topics -}}
{{ if .HasErrors }} [ERROR]  Create/Update Topic {{ .Name }} failed with errors: 
  {{ range .Errors }}
  - {{ . }} 
  {{ end -}} 
{{ else -}}
Topic {{ .Name }} Create/Updated. Configs:
  {{ range .ChangedConfigs }} 
  - Config {{ .Name }} changed from {{ .OldVal }} to {{ .NewVal }}
  {{- end }} 
{{ end -}}
{{ end -}}

{{ range .Clients -}}
{{ if $.IsPlan }}[PLAN]{{ end }}. Add role {{ .Role }} to principal {{ .Principal }} for {{ .ResourceType }}:{{ .ResourceName }} with type {{ .PatternType }}
{{ end }}

