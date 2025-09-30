terraform-docs markdown table . --output-file README.md
terraform-docs markdown table .\modules\clusters --output-file README.md
terraform-docs markdown table .\modules\mounts --output-file README.md

type README.md  ".\modules\clusters\README.md"  ".\modules\mounts\README.md"  > final.md