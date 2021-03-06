import org.gradle.kotlin.dsl.*

plugins {
    java
    kotlin("jvm")
    id ("org.openjfx.javafxplugin") version "0.0.8"
}

javafx {
    version = "11"
    modules("javafx.controls")
}

dependencies {

    compile(project(":completable-reactor-model"))

    compile(Libs.kotlin_jdk8)
    compile(Libs.kotlin_stdlib)
    compile(Libs.kotlin_reflect)
    compile(Libs.kotlin_logging)


    testImplementation(Libs.junit_api)
    testRuntimeOnly(Libs.junit_engine)
    testRuntimeOnly(Libs.slf4j_simple)
}
