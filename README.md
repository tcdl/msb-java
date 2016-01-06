# MSB-Java [![Project status](https://travis-ci.org/tcdl/msb-java.svg?branch=master)](https://travis-ci.org/tcdl/msb-java) [![Coverage Status](http://img.shields.io/coveralls/tcdl/msb-java/master.svg)](https://coveralls.io/r/tcdl/msb-java?branch=master)

Microservice bus - Java API. See [developer guide](/doc/MSB.md) for motivation, architecture and usage.

## Required tools:
* JDK (8 or higher)
* Maven (version 3), main build tool

## Bintray support release configuration:
If you're part of the tcdl bintray organization (https://bintray.com/tcdl) and have sufficient rights you can publish releases to bintray (https://bintray.com/tcdl/releases/msb-java/view).

For this you'll need to add a `server` to the `servers` section of your settings.xml:
```
<server>
  <id>bintray-msb-java</id>
  <username>[YOUR_USERNAME]</username>
  <password>[YOUR_API_TOKEN]</password>
</server>
```
## Bintray / jcenter SNAPSHOT publishing configuration:
If you're part of the tcdl bintray organization (https://bintray.com/tcdl) and have sufficient rights you can publish snapshots to jfrog / jcenter (http://oss.jfrog.org/artifactory/simple/oss-snapshot-local/io/github/tcdl/).

For this you'll need to add a `server` to the `servers` section of your settings.xml:
```
<server>
  <id>oss-jfrog-msb-java</id>
  <username>[YOUR_BINTRAY_USERNAME]</username>
  <password>[YOUR_BINTRAY_API_TOKEN]</password>
</server>
```

## Java code style
### Downloadable formatting:
**Eclipse**: [codeFormatting.xml](/settings/codeFormatting.xml) -> Window -> Preferences -> **Java -> Code Style -> Formatter** -> Import

**IntelliJ IDEA:** Use the same formatting XML as for Eclipse and use [IntelliJ IDEA 13: Importing Code Formatter Settings from Eclipse](http://blog.jetbrains.com/idea/2014/01/intellij-idea-13-importing-code-formatter-settings-from-eclipse/).

Additionally disable folding of imports (via '\*') in **Intellij IDEA.** Navigate to Settings -> Editor -> Code Style -> Java -> Imports, set "Class count to use import with '\*'" and "Names count to use static import with '\*'" both to 1000.

### How codeFormatting.xml was created:
In **Eclipse:** As base, use "Java Conventions [built-in]", after that apply the following:

- Indentation / Tab policy = **Spaces only**
- Indentation / Indentation size = **4**
- Indentation / Tab size = **4**
- Indentation / Indent -> all sub checkboxes checked, **except for "Empty lines"**
- Line Wrapping / Maximum line width = **160**
- Line Wrapping / Never join already wrapped lines: **true**
- Comments / Maximum line width for comments = **160**
- Comments / Enable block comment formatting = **OFF**
- Comments / Enable line comment formatting = **OFF**
- Comments / New line after @param tags = **OFF**
- Comments / Never indent block comments on first column: **OFF**
- Comments / Never indent line comments on first column: **OFF**
- Braces / Brace positions: **all "Same line"**
- White space: no changes
- Blank lines: no changes
