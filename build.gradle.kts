import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.tasks.testing.logging.TestLogEvent.*

plugins {
  java
  application
  id("com.github.johnrengelman.shadow") version "5.2.0"
}

group = "ai.brace"
version = "1.0.0-SNAPSHOT"

repositories {
  mavenCentral()
}

val vertxVersion = "3.9.4"
val junitJupiterVersion = "5.6.0"
val log4jVersion = "2.13.3"
val jetbrainsAnnotationsVersion = "20.1.0"

val mainVerticleName = "ai.brace.transaction_test.MainVerticle"
val watchForChange = "src/**/*"
val doOnChange = "./gradlew classes"
val launcherClassName = "io.vertx.core.Launcher"

application {
  mainClassName = launcherClassName
  applicationDefaultJvmArgs += "-Dlog4j.configurationFile=log4j2.xml"
}

dependencies {
  implementation("io.vertx:vertx-pg-client:$vertxVersion")
  implementation("org.apache.logging.log4j:log4j-api:$log4jVersion")
  implementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
  implementation("org.apache.logging.log4j:log4j-slf4j-impl:$log4jVersion")
  implementation("org.jetbrains:annotations:$jetbrainsAnnotationsVersion")


  testImplementation("io.vertx:vertx-junit5:$vertxVersion")
  testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
}

  java {
    sourceCompatibility = JavaVersion.VERSION_15
    targetCompatibility = JavaVersion.VERSION_15
  }

tasks.withType<ShadowJar> {
  archiveClassifier.set("fat")
  manifest {
    attributes(mapOf("Main-Verticle" to mainVerticleName))
  }
  mergeServiceFiles {
    include("META-INF/services/io.vertx.core.spi.VerticleFactory")
  }
}

tasks.withType<Test> {
  useJUnitPlatform()
  testLogging {
    events = setOf(PASSED, SKIPPED, FAILED)
  }
}

tasks.withType<JavaExec> {
  args = listOf("run", mainVerticleName, "--redeploy=$watchForChange", "--launcher-class=$launcherClassName", "--on-redeploy=$doOnChange")
}
