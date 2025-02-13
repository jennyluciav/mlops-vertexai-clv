bucket_name  = "creditcards" # -env will be added as suffix
dataset_name = "creditcards"
environment  = "dev"
groups = {
  gcp-ml-ds     = null
  gcp-ml-eng    = null
  gcp-ml-viewer = null
}

# env will be added as branch name
github = {
  organization = "jennyluciav"
  repo         = "mlops-vertexai"
}

# Additional labels. env label will be added automatically
labels = {
  "team" : "ml"
}

notebooks = {
  "nb" : {
    type = "USER_MANAGED"
  },
}

prefix = "dev"
project_config = {
  #billing_account_id = "019ADF-3246AE-C2DB63" # Use only billing BA if it is required to create the project
  #parent             = "folders/261046259366"
  project_id         = "fsa-sandbox" # -env will be added as suffix
}
region = "europe-west2"


service_encryption_keys = {
  aiplatform    = "projects/CMEK_PROJECT_ID/locations/europe-west2/keyRings/europe-west2/cryptoKeys/storage"
  bq            = "projects/CMEK_PROJECT_ID/locations/europe-west2/keyRings/europe-west2/cryptoKeys/bq"
  notebooks     = "projects/CMEK_PROJECT_ID/locations/europe-west2/keyRings/europe-west2/cryptoKeys/storage"
  secretmanager = "projects/CMEK_PROJECT_ID/locations/europe-west2/keyRings/europe-west2/cryptoKeys/storage"
  storage       = "projects/CMEK_PROJECT_ID/locations/europe-west2/keyRings/europe-west2/cryptoKeys/storage"
}

