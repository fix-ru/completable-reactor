object Vers {
    val kotlin = "1.2.61"
    val sl4j = "1.7.25"
    val dokkav = "0.9.16"
    val gradleReleasePlugin = "1.2.20"
    val junit = "5.2.0"

    val aggregatingProfiler = "1.4.7"
    val dynamicProperty = "1.0.5"

    val spring = "5.0.8.RELEASE"
}

object Libs {
    val kotlin_stdlib = "org.jetbrains.kotlin:kotlin-stdlib:${Vers.kotlin}"
    val kotlin_jdk8 = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:${Vers.kotlin}"
    val kotlin_reflect = "org.jetbrains.kotlin:kotlin-reflect:${Vers.kotlin}"

    val gradleReleasePlugin = "ru.fix:gradle-release-plugin:${Vers.gradleReleasePlugin}"
    val dokkaGradlePlugin = "org.jetbrains.dokka:dokka-gradle-plugin:${Vers.dokkav}"

    val slf4j_api = "org.slf4j:slf4j-api:${Vers.sl4j}"
    val slf4j_simple = "org.slf4j:slf4j-simple:${Vers.sl4j}"

    val mockito = "org.mockito:mockito-all:1.10.19"
    val mockito_kotiln = "com.nhaarman:mockito-kotlin-kt1.1:1.6.0"
    val kotlin_logging = "io.github.microutils:kotlin-logging:1.5.9"

    val junit_api = "org.junit.jupiter:junit-jupiter-api:${Vers.junit}"
    val junit_parametri = "org.junit.jupiter:junit-jupiter-params:${Vers.junit}"
    val junit_engine = "org.junit.jupiter:junit-jupiter-engine:${Vers.junit}"


    val aggregatingProfiler = "ru.fix:aggregating-profiler:${Vers.aggregatingProfiler}"
    val dynamicProperty = "ru.fix:dynamic-property-api:${Vers.dynamicProperty}"

    val springTest = "org.springframework:spring-test:${Vers.spring}"
    val springBeans = "org.springframework:spring-beans:${Vers.spring}"
    val springContext = "org.springframework:context:${Vers.spring}"
}