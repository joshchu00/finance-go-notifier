@Library("library") _

jBuild {
  [
    gitURL = 'https://github.com/joshchu00/finance-go-notifier.git',
    gitBranch = 'develop',
    buildLanguage = 'go',
    buildImage = 'joshchu00/go-build-kafka:1.11.5-alpine',
    dockerName = 'joshchu00/finance-go-notifier',
    downstreamJob = 'finance-deployment'
  ]
}
