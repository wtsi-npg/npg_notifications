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

The current version implements notifications for PacBio sequencing platform
customers.

## Running the scripts

To register recently QC-ed entities as tasks with `porch`

```bash
qc_state_notification register --conf_file_path path/to/qc_state_app_config.ini
```

To process one `porch` task

```bash
qc_state_notification process --conf_file_path path/to/qc_state_app_config.ini
```

Processing includes claiming one task, sending per-study emails and updating the
status of the `porch` task to `DONE`.

The test data directory has an example of a [configuration file](tests/data/qc_state_app_config.ini).