# npg_notifications

A utility for notifying customers about lifecycle events in the analysis
and QC of sequencing data.

[porch](https://github.com/wtsi-npg/npg_porch) service is used to queue and
process (send) notifications. The notification producer can repeatedly send
to `porch` the same notification. `porch` guarantees that the repeated
notifications are not kept and therefore not processed.

The consumer of the notifications is responsible for sending the message
to the customer. For a fully automated system the consumer should implement
the correct protocol for dealing with failed attempts to notify the customer.

If different types of notification (for example, an e-mail and a MQ message)
have to be sent for the same event, it is advised either to use a separate
`porch` pipeline for each type of notification or to include additional
information about the notification protocol and format into the payload that
is sent to `porch`.

## Scope

The current version implements notifications for PacBio and ONT sequencing platform
customers.

## PacBio

### Running the scripts

To register recently QC-ed entities as tasks with `porch`

```bash
npg_qc_state_notification register --conf_file_path path/to/qc_state_app_config.ini
```

To process one `porch` task

```bash
npg_qc_state_notification process --conf_file_path path/to/qc_state_app_config.ini
```

Processing includes claiming one task, sending per-study emails and updating the
status of the `porch` task to `DONE`.

The test data directory has an example of a [configuration file](tests/data/qc_state_app_config.ini).

### ONT

#### Configuration

Configuration requires an INI-format file with the following sections:

```ini
[MySQL MLWH]
dbhost = <MySQL host>
dbport = <MySQL port>
dbuser = <MySQL user>
dbpassword = <MySQL password>
dbschema = <MySQL schema>

[PORCH]
url = <Porch server http URL>
admin_token = <Porch server admin authentication token>
pipeline_token = <Porch server pipeline authentication token>

[MAIL]
domain = <email FQDN>
```

The first time a new pipeline is created on a particular `porch` service, the following two
steps are requiredL

- Register the pipeline with `porch`:

```bash
 npg_ont_event_notification --verbose --colour --conf-file path/to/config.ini register
```

This creates a new record of the pipeline and its version in the `porch` database. This
step requires an admin token for the `porch` service to be in the configuration file. The
`--verbose` and `--colour` flags are optional, but help to see the progress of the script.

- Obtain a new pipelne authentication token:

```bash
 npg_ont_event_notification --verbose --colour --conf-file path/to/config.ini token
```

This creates a new token and prints it to STDOUT. Once the token is obtained, it should be
added to the configuration file. The token cannot be retrieved again. This token is used to
authenticate with the `porch` service when submitting and running tasks.

#### Running the script

There are two parts to the notification process:

1. Finding new ONT runs of interest and adding them to the notification pipeline as tasks.
2. Processing the tasks and sending the notifications.

Each of these steps is typically run as a separate cron job. Adding the same task multiple
times is safe, as `porch` will process is only once.

- Find new ONT runs and add tasks:

```bash
<run disocvery script> | npg_ont_event_notification --verbose --colour --conf-file path/to/config.ini --log-config path/to/logging.ini add
```

The discovery script can be anything that writes [baton](http://wtsi-npg.github.io/baton)
format JSON to STDOUT, one JSON object per line e.g. the `locate-data-objects` script from
https://github.com/wtsi-npg/npg-irods-python . The JSON must include the metadata associated
with the path it represents.

- Process tasks:

```bash
npg_ont_event_notification --conf-file path/to/config.ini --log-config path/to/logging.ini run
```

This will claim up to 100 tasks from the `porch` service, process them and mark them as done
if they are successful, or retry them if they fail. The claim size is fixed and should be
sufficient to clear all tasks each time the script is run.
