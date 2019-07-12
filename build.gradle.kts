import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.3.41"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("io.reactivex.rxjava2:rxjava:2.2.10")

    testImplementation("junit:junit:4.12")
    testImplementation("com.google.truth:truth:1.0")
}

tasks.withType(KotlinCompile::class.java).all {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_1_8.toString()
    }
}
