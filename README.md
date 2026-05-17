## Install Java 17

```bash id="7ehqgb"
sudo apt update
sudo apt install openjdk-17-jdk maven -y
```

---

## Verify Java + Maven

```bash id="sgmbmw"
java -version
mvn -v
```

---

## Configure Java 17

```bash id="l6tk3j"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Persist it:

```bash id="w4jvs6"
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

## Build the Project

From the repository root:

```bash id="n0u5rb"
mvn clean install
```

---



## Run `Servers.java`

```bash id="tk0g0n"
mvn exec:java -Dexec.mainClass="org.example.Servers.java"
```

If `Servers.java` belongs to a package, use the fully qualified class name:

```bash id="6czlln"
mvn exec:java -Dexec.mainClass="com.project.Servers"
```

---
