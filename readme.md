## Belk Code Skills

This repository contains the code for the Belk skills.

## Setup

This repository contains a .envrc file for use with direnv. With that installed you should have a separate python interpreter that direnv's hook will activate for you when you cd into this repository.

Once you have direnv set up and activating inside the repo, just `make` to install dev dependencies and get started.

Note that the platform_constraints.txt file is used to generate the requirements.txt file. If you add any new packages to the platform, you'll need to make sure copy + paste MaxServer/setup/requirements.txt into platform_constraints.txt. This is to ensure that dependencies are the same when testing locally vs on the platform.

## Development

To run the skill locally, refer to the skill-framework [README](https://github.com/answerrocket/skill-framework/tree/main).