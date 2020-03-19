# sensorpush_datadog
Grab sensorpush metrics and emit to Datadog

Datadog API and APP keys need to be set with the `DD_API_KEY` and `DD_APP_KEY` environment variables.

Sensorpush email and password need to be set with the `SENSORPUSH_EMAIL` and `SENSORPUSH_PASSWORD` environment variables.

This project uses Postgres to make sure that points are not re-submitted. Yes, I know, it's not efficient, this is still a work in progress.
You will need to have a postgres user with the username `sensorpush` and a database `sensorpush`. The table creation is listed in the main `sensorpush_monitor.rb`. 
The password is passed in via the `SENSORPUSH_PG_PASSWORD` if you are using password authentication.
