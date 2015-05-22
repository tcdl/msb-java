MSB-Java ![Project status](https://travis-ci.org/tcdl/msb-java.svg?branch=master)
===========

Microservice bus - Java API

Required tools:
---------------
* JDK (8 or higher)
* Maven (version 3), main build tool

Bintray support release configuration:
--------------------------------------
If you're part of the tcdl bintray organization (https://bintray.com/tcdl) and have sufficient rights you can publish releases to bintray (https://bintray.com/tcdl/releases/msb-java/view).

For this you'll need to add a `server` to the `servers` section of your settings.xml:
```
<server>
  <id>bintray-msb-java</id>
  <username>[YOUR_USERNAME]</username>
  <password>[YOUR_API_TOKEN]</password>
</server>
```
