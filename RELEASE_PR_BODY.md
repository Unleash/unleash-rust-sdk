## Release 0.16.0

Bumps both crates from 0.16.0-beta.1 to 0.16.0.

### Changes
- chore: ignore previous betas for calculating next release version (#127)
- fix: use online cargo mutation in release CI so that the dependencies we need are present (#125)
- docs: migration guide for 0.16.0 (#124)
- chore: steal release pr generation from Unleash Edge CI (#122)
- chore: beta release to test out the public API in a scaffold (#123)
- refactor!: rework http layers to simplify the public APIs (#121)
- feat: add support for reqwest-13 (#120)
- feat: rexport feature key macro (#119)
- chore: fix publish pipeline missing pipes
- Remove validation and test steps from publish.yaml
- chore: patch release process (#117)
- fix: allow tests to work on beta releases (#116)
- chore: rework publish pipeline (#115)
- chore: feature key proc macro (#113)
- chore: move SDK into a sub crate (#112)
- feat: yggdrasil compatbile custom strategies (#111)
- chore: swap in yggdrasil (#110)
- update docs link (#108)
