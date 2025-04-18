export PGHOST=azpgdbsvr01.postgres.database.azure.com
export PGUSER=odl_user_1668166@cloudevents.ai
export PGPORT=5432
export PGDATABASE=postgres
export PGPASSWORD="$(az account get-access-token --resource https://ossrdbms-aad.database.windows.net --query accessToken --output tsv)" 
psql
