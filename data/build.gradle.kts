plugins {
    java
    `java-library`
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":domain"))
    implementation(project(":lib:api"))
    implementation(project(":lib:v1"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.9.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
}
