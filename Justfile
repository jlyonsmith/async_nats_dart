# Justfile for async_nats project
list:
    just --list

# Run the example, which starts a NATS server and runs the example client
example:
    #!/usr/bin/env fish
    source ./common.fish
    nats-server -DV &
    dart run example/main.dart
    if test $status -eq 0
        info "SUCCESS!"
    end
    kill $last_pid

# Run all tests
test:
    dart test

# Do a release, building backend and UI and tagging the source code
release OPERATION='incrMinor':
    #!/usr/bin/env fish
    source ./common.fish

    set root_file "version.json5"

    if test ! -e $root_file
        error "$root_file file not found"
        exit 1
    end

    info "Checking for uncommitted changes"

    if not git diff-index --quiet HEAD -- > /dev/null 2> /dev/null
        error "There are uncomitted changes - commit or stash them and try again"
        exit 1
    end

    set branch (string trim (git rev-parse --abbrev-ref HEAD 2> /dev/null))
    set name (basename (pwd))

    info "Starting release of '$name' on branch '$branch'"

    info "Checking out '$branch'"
    git checkout $branch

    info "Pulling latest from remote"
    git pull

    set rollback "git checkout $branch .; "

    stampver -u {{OPERATION}}

    if test $status -ne 0
        error "Unable to generation version information"
        exit 1
    end

    set tagName (cat "scratch/version.tag.txt")
    set tagDescription (cat "scratch/version.desc.txt")

    git rev-parse $tagName > /dev/null 2> /dev/null
    if test $status -ne 0; set isNewTag 1; end

    if set -q isNewTag
        info "'"$tagName"' is a new tag"
    else
        warning "Tag '"$tagName"' already exists and will not be moved"
    end

    info "Building and test package"
    dart test
    if test $status -ne 0
        eval $rollback
        exit 1
    end

    info "Staging changes"
    git add :/

    info "Committing version changes"
    git commit -m $tagDescription

    if set -q isNewTag
        info "Tagging"
        git tag -a $tagName -m $tagDescription
    end

    info "Pushing to 'origin'"
    git push --follow-tags

    info "Finished release of '$name' on branch '$branch'"
    exit 0

# Delete the last tag; used when the release build goes horribly wrong
del-last-tag:
  #!/usr/bin/env fish
  set tagName (cat "scratch/version.tag.txt")

  git tag -d $tagName
  git push origin --delete $tagName

