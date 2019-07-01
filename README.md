# data-pipelines-plugin-template

A template used for DSP SDK plugins. This project will generate a skeleton plugin that can be further modified
to create a custom plugin that runs in a DSP pipeline.

## Build Requirements

* Java 8
* Network access to repo.splunk.com

## Getting Started

* Clone this repository
* Run project setup using gradle:

```
./gradlew expandTemplates -PSDK_FUNCTIONS_PATH=examples
```

Note that `expandTemplates` should generally only be run once. It can be run again, but will overwrite any existing files
with the same names.

* Compile:

```
./gradlew build
```

The plugin jar artifact will be found in `build/libs`.

* Commit to git, e.g.:

```
git add src/ && git commit
```

* Remove the git origin to avoid pushing back to this repository

```
git remote rm origin
```

* Push to a new repository and iterate as necessary

## Register and Upload Plugin

In order to use your newly created plugin function in a DSP pipeline, you must first register and upload it.

First, build the JAR file:
```
./gradlew build
```

Next, configure Gradle to communicate with DSP by modifying plugin upload configuration in `gradle.properties`:

`SCLOUD_TOKEN_FILE` - Path to a text file containing only the value of the `access_token` field in the response from `scloud login`. This value should not have quotes around it.

`PLUGIN_UPLOAD_SERVICE_PROTOCOL` - `http` or `https`

`PLUGIN_UPLOAD_SERVICE_HOST` - Typically, the hostname used to reach the DSP API

`PLUGIN_UPLOAD_SERVICE_PORT` - Typically, the port used by the DSP API (ex. `443`)

`PLUGIN_UPLOAD_SERVICE_ENDPOINT` - Path to the DSP plugins endpoint (ex. `streams/v1/plugins`)

Then, register the plugin with DSP:
```
./gradlew registerPlugin -PSDK_PLUGIN_NAME="sdk-examples" -PSDK_PLUGIN_DESC="Template SDK example functions."
```

Note the `plugin_id` in the response. You can also list all plugins with their respective `plugin_id`s at anytime by running `./gradlew getPlugins`.

Finally, upload the plugin JAR, passing the `plugin_id` as a command line parameter:
```
./gradlew uploadPlugin -PPLUGIN_ID=<id>
```
