------
## Topics
{{ range .Topics -}}
{{ if .HasErrors }}[ERROR]{{ if $.IsPlan }}[PLAN]{{end}}  Create/Update Topic {{ .Name }} {{if $.IsPlan}} would have {{end}}failed with errors: 
{{- range .Errors }}
  - {{ . }} 
{{ end -}} 
{{ else -}}
{{ if $.IsPlan }}[Plan] Will {{if .IsNew}}Create{{else}}Update{{end}} {{ else }} {{if .IsNew}}Created{{else}}Update{{end}} {{ end }} Topic {{ .Name }}. {{ if .HasChangedConfigs}}Non-default configs:{{else}}(default configs){{end}}
  {{- range .ChangedConfigs }} 
  - Config {{ .Name }} changed from {{ .OldVal }} to {{ .NewVal }}
  {{- end }} 
{{ if .PartitionsChanged -}} 
{{ if $.IsPlan }}[PLAN] Partitions will be changed{{else}} Partitions changed{{end}} to {{.NewPartitions}} from {{.OldPartitions}} New plan is 
  | Partition | Brokers |
{{- range $partition, $brokers:=  .ReplicaPlan }}
  | {{ $partition }} | {{range $broker := $brokers}}{{$broker}},{{end}} |
{{- end}}
{{- end }}
{{ end -}}
{{ end -}}
## Schemas
{{ range .Schemas -}}
{{ if .Changed -}} 
   {{ if $.IsPlan -}}[PLAN] -  Subject {{ .SubjectName }} will be registered with a new version.
   {{ else }} Subject {{ .SubjectName }} changed ({{ .Changed }})registered with new version {{ if eq .NewVersion 0 }}(Known after apply){{ else }}{{ .NewVersion }}{{ end}}
   {{- end }}
  {{ end -}}
{{ end }}
## Roles and Clients
{{ range .Clients -}}
{{ if $.IsPlan }}[PLAN]. Will add{{ else }} Added {{ end }} role {{ .Role }} to principal {{ .Principal }} for {{ .ResourceType }}:{{ .ResourceName }} with type {{ .PatternType }}
{{ end }}

