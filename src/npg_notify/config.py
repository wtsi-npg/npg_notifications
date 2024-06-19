import configparser
import pathlib
import json

"""Common utility functions for the package."""

DEFAULT_CONF_FILE_TYPE = "ini"


def get_config_data(conf_file_path: str, conf_file_section: str = None):
    """
    Parses a configuration file and returns its content.

    Allows for two types of configuration files, 'ini' and 'json'. The type of
    the file is determined from the extension of the file name. In case of no
    extension an 'ini' type is assumed.

    Args:

      conf_file_path:
        A configuration file with database connection details.
      conf_file_section:
        The section of the configuration file. Optional. Should be defined
        for 'ini' files.

    Returns:
      For an 'ini' file returns the content of the given section of the file as
      a dictionary.
      For a 'json' file, if the conf_file_section argument is not defined, the
      content of a file as a Python object is returned. If the conf_file_section
      argument is defined, the object returned by the parser is assumed to be
      a dictionary that has the value of the 'conf_file_section' argument as a key.
      The value corresponding to this key is returned.
    """

    conf_file_extention = pathlib.Path(conf_file_path).suffix
    if conf_file_extention:
        conf_file_extention = conf_file_extention[1:]
    else:
        conf_file_extention = DEFAULT_CONF_FILE_TYPE

    if conf_file_extention == DEFAULT_CONF_FILE_TYPE:
        if not conf_file_section:
            raise Exception(
                "'conf_file_section' argument is not given, "
                "it should be defined for '{DEFAULT_CONF_FILE_TYPE}' "
                "configuration file."
            )

        config = configparser.ConfigParser()
        config.read(conf_file_path)

        return {i[0]: i[1] for i in config[conf_file_section].items()}

    elif conf_file_extention == "json":
        conf: dict = json.load(conf_file_path)
        if conf_file_section:
            if isinstance(conf, dict) is False:
                raise Exception(f"{conf_file_path} does not have sections.")
            if conf_file_section in conf.keys:
                conf = conf[conf_file_section]
            else:
                raise Exception(
                    f"{conf_file_path} does not contain {conf_file_section} key"
                )

        return conf

    else:
        raise Exception(
            f"Parsing for '{conf_file_extention}' files is not implemented"
        )
