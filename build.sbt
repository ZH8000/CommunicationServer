import AssemblyKeys._ // put this at the top of the file

assemblySettings

name := "CommunicationServer"

version := "0.0.4"

scalaVersion := "2.11.2"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "commons-daemon" % "commons-daemon" % "1.0.15",
  "ch.qos.logback" % "logback-classic" % "1.1.2",
  "com.rabbitmq" % "amqp-client" % "3.3.5",
  "org.mongodb" %% "casbah" % "2.7.3",
  "com.jsuereth" %% "scala-arm" % "1.4",
  "com.sun.mail" % "javax.mail" % "1.5.2"
)


