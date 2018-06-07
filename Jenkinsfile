#!/usr/bin/env groovy

final Integer RETRY = 3
final Integer TIMEOUT_TIME_CHECKOUT = 5
final Integer TIMEOUT_TIME_COMPILE = 20
final Integer TIMEOUT_TIME_PIPELINE = 60
final Integer TIMEOUT_TIME_TEST = 60
final String TIMEOUT_UNIT = 'MINUTES'

// Docker images containing OpenJDK 7 or 8
final String JDK7_ALPINE_IMAGE = 'openjdk:7u121-jdk'
final String JDK8_ALPINE_IMAGE = 'openjdk:8-jdk-alpine'

pipeline {
  agent any

  options {
    retry(RETRY)
    timeout(time: TIMEOUT_TIME_PIPELINE, unit: TIMEOUT_UNIT)
    timestamps()
  }

  stages {
    stage('Checkout') {
      options {
        retry(RETRY)
        timeout(time: TIMEOUT_TIME_CHECKOUT, unit: TIMEOUT_UNIT)
      }
      steps {
        checkout scm

        script {
          sh 'git --version'
          env.GIT_COMMIT = sh(returnStdout: true, script: "git log -n 1 --pretty=format:'%H'").trim()
        }

        // If any gradle home already exists, remove it to isolate builds.
        // We do this at the start rather than the end so we can see the state following a build.
        script {
          cleanGradleHome(JDK7_ALPINE_IMAGE)
        }
      }
    }

    stage('Expand Templates') {
      options {
        timeout(time: TIMEOUT_TIME_COMPILE, unit: TIMEOUT_UNIT)
      }
      steps {
        script {
          gradleCompile(JDK7_ALPINE_IMAGE)
        }
      }
    }

    stage('Build Compile') {
      options {
        timeout(time: TIMEOUT_TIME_COMPILE, unit: TIMEOUT_UNIT)
      }
      steps {
        script {
          gradleCompile(JDK7_ALPINE_IMAGE)
        }
      }
    }

    stage('Build UnitTests Java7') {
      options {
        timeout(time: TIMEOUT_TIME_TEST, unit: TIMEOUT_UNIT)
      }
      steps {
        script {
          gradleUnitTestOnly(JDK7_ALPINE_IMAGE)
        }
      }
    }

    stage('Build UnitTests Java8') {
      options {
        timeout(time: TIMEOUT_TIME_TEST, unit: TIMEOUT_UNIT)
      }
      steps {
        script {
          gradleUnitTestOnly(JDK8_ALPINE_IMAGE)
        }
      }
    }

  } //stages
} // pipeline

/**
 * Run a set of commands within a Docker image
 * @param imageName name of Docker image to run
 * @param commands list of commands to run (each command is a full command-line string)
 * @param insideArgs any other arguments to the docker command when running
 * @return result of the docker.image.inside Closure
 */
def shellInsideImage(String imageName, Iterable<String> commands, String insideArgs = "") {
  docker.image(imageName).inside("-u root ${insideArgs}") {
    for (String command in commands) {
      sh command
    }
  }
}

def cleanGradleHome(String imageName) {
  // run this within a container so that it has permission to delete the directory
  shellInsideImage(imageName, ['rm -rf gradle-local-home'])
}

def gradleExpandTemplates(String imageName) {
  shellInsideImage(imageName, [createGradleCommand("expandTemplates -PSDK_FUNCTION_TYPE=streaming -PSDK_CLASS_NAME=ReadStuff -PSDK_FUNCTION_NAME=read-stuff -PSDK_UI_NAME='Read Stuff'")])
  shellInsideImage(imageName, [createGradleCommand("expandTemplates -PSDK_FUNCTION_TYPE=scalar -PSDK_CLASS_NAME=ConvertStuff -PSDK_FUNCTION_NAME=convert-stuff -PSDK_UI_NAME='Convert Stuff'")])
}

def gradleCompile(String imageName) {
  shellInsideImage(imageName, [createGradleCommand("clean compileJava compileTestJava --continue")])
}

def gradleUnitTestOnly(String imageName) {
  // only run the tests; ensure we do not recompile
  shellInsideImage(imageName, [createGradleCommand("test -x compileJava -x compileTestJava --continue")])
}

/**
 * - Disable the gradle daemon because it will not persist between runs within a container
 * - Set the gradle-user-home so that dependencies are downloaded once per build for all stages
 * @param command gradle tasks and flags to enrich
 * @return runnable gradle command including boilerplate flags
 */
String createGradleCommand(String command) {
  return "./gradlew -Dorg.gradle.daemon=false --gradle-user-home gradle-local-home " + command
}

def notifyBitbucket() {
  step([$class: 'StashNotifier'])
}
