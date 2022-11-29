# Releasing

This document describes how to release a new version `X.Y.Z` for maintainers.
It would be required to replace `X.Y.Z` with the actual release version.

## 1. Create a new branch

Create a new branch `release/vX.Y.Z` from `master` branch like the following:
```shell
git checkout master
git pull origin
git checkout -b release/vX.Y.Z
```

## 2. Update `CHANGELOG.md`

1. Add a section of the new release version `vX.Y.Z`  
    We recommend you add one of the following links to this section.
    * `https://github.com/lerna-stack/akka-entity-replication/compare/vA.B.C...vX.Y.Z` if this release is a successor.
      * `A.B.C` is the previous latest version.
    * `https://github.com/lerna-stack/akka-entity-replication/tree/vX.Y.Z` if this release is the first one.
2. Update the unreleased version link to `https://github.com/lerna-stack/akka-entity-replication/compare/vX.Y.Z...master`

## 3. Commit & Push

Commit changes, and then push the branch like the following:
```shell
git commit --message 'release vX.Y.Z'
git push origin
```

## 4. Create a Pull Request

After committing and pushing all changes, create a pull request.
Other maintainers will review and merge the pull request.

## 5. Push a new version tag `vX.Y.Z`

*It is highly recommended ensuring that the new version tag is correct.*  
*The CI will automatically publish this release when detecting the version tag.*

Create and push the new version tag `vX.Y.Z` like the following:
```shell
git checkout master
git tag vX.Y.Z
git push origin vX.Y.Z
```

## 6. Check the release is available

Check the release is available at [Maven Central Repository](https://repo1.maven.org/maven2/com/lerna-stack/).

**NOTE**
- The release will be available about 10 minutes after publishing.
- It requires more time to be able to find the release with searching (about 2 hours max).

## 7. Publish GitHub Pages

Publish GitHub Pages using `sbt-site` and `sbt-ghpages`.
To publish the pages, checkout the new version tag `vX.Y.Z` and then run the below command.
Note that you should have proper permission to publish.
```shell
sbt ghpagesPushSite
```

## 8. Create a new release `vX.Y.Z`

Create a new release `vX.Y.Z` from [this link](https://github.com/lerna-stack/akka-entity-replication/releases/new).

- **Choose a tag**: select the new version tag
- **Release title**: the same as the tag
- **Describe this release**:  
    Write the following text, at least.  
    Replace the part `#vXYZ---YYYY-MM-DD` of the link with the actual release version and date.
    ```markdown
    See [CHANGELOG] for details.
  
    [CHANGELOG]: https://github.com/lerna-stack/akka-entity-replication/blob/master/CHANGELOG.md#vXYZ---YYYY-MM-DD
    ```
