scala-project:
	mvn archetype:generate \
			-DarchetypeGroupId=org.apache.flink \
			-DarchetypeArtifactId=flink-quickstart-scala \
			-DarchetypeVersion=1.9.1 \
			-DgroupId=me.vipmind.flinker \
			-DartifactId=flinker-scala \
			-Dversion=1.0-SNAPSHOT \
			-Dpackage=me.vipmind.flinker \
			-DinteractiveMode=false

java-project:
	mvn archetype:generate \
			-DarchetypeGroupId=org.apache.flink \
			-DarchetypeArtifactId=flink-quickstart-java \
			-DarchetypeVersion=1.9.1 \
			-DgroupId=me.vipmind.flinker \
			-DartifactId=flinker-java \
			-Dversion=1.0-SNAPSHOT \
			-Dpackage=me.vipmind.flinker \
			-DinteractiveMode=false
