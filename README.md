# data-pipelines-plugin-template

A template used for BLAM SDK plugins. This project will generate a skeleton plugin that can be further modified
to create a custom plugin that runs in a BLAM pipeline.

## Build Requirements

* Java 7
* Network access to repo.splunk.com

## Getting Started

* Clone this repository
* Run project setup using gradle:

```
./gradlew expandTemplates \
  -PSDK_CLASS_NAME=MyFunc \
  -PSDK_FUNCTION_NAME=my-func \
  -PSDK_UI_NAME="My Function"
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