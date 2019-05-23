//"Jenkins Pipeline is a suite of plugins which supports implementing and integrating continuous delivery pipelines into Jenkins. Pipeline provides an extensible set of tools for modeling delivery pipelines "as code" via the Pipeline DSL."
//More information can be found on the Jenkins Documentation page https://jenkins.io/doc/
library 'github-utils-shared-library@master'
@Library('github.com/connexta/cx-pipeline-library@master') _

pipeline {
    agent {
        node {
            label 'linux-large'
            customWorkspace "/jenkins/workspace/${JOB_NAME}/${BUILD_NUMBER}"
        }
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: BRANCH_NAME == "11.x" ? '15' : '3'))
        disableConcurrentBuilds()
        timestamps()
    }
    environment {
        DOCS = 'distribution/docs'
        ITESTS = 'distribution/test/itests/test-itests-ddf'
        POMFIX = 'libs/pom-fix-run'
        LARGE_MVN_OPTS = '-Xmx8192M -Xss128M '
        DISABLE_DOWNLOAD_PROGRESS_OPTS = '-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn '
        LINUX_MVN_RANDOM = '-Djava.security.egd=file:/dev/./urandom'
        COVERAGE_EXCLUSIONS = '**/test/**,**/itests/**,**/*Test*,**/sdk/**,**/*.js,**/node_modules/**,**/jaxb/,**/wsdl/,**/nces/sws/**,**/*.adoc,**/*.txt,**/*.xml'
        GITHUB_USERNAME = 'connexta'
        GITHUB_REPONAME = 'ddf-yorktown'
    }
    stages {
        stage('Setup') {
            steps {
                retry(3) {
                    checkout scm
                }
                dockerd {}
                slackSend color: 'good', message: "STARTED: ${JOB_NAME} ${BUILD_NUMBER} ${BUILD_URL}"
                withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                    postCommentIfPR("Internal build has been started. Your results will be available at completion. See build progress in [legacy Jenkins UI](${BUILD_URL}) or in [Blue Ocean UI](${BUILD_URL}display/redirect).", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${GITHUB_TOKEN}")
                    script {
                        //Check if the current HEAD is a Merge commit https://stackoverflow.com/questions/3824050/telling-if-a-git-commit-is-a-merge-revert-commit/3824122#3824122
                        try  {
                            sh(script: 'if [ `git cat-file -p HEAD | head -n 3 | grep parent | wc -l` -gt 1 ]; then exit 1; else exit 0; fi')
                            //No error was thrown -> we called exit 0 -> HEAD is not a merge commit/doesn't have multiple parents
                            env.PR_COMMIT = sh(returnStdout: true, script: 'git rev-parse HEAD').trim()
                        } catch (err) {
                            //An error was thrown -> we called exit 1 -> HEAD is a merge commit/has multiple parents
                            env.PR_COMMIT = sh(returnStdout: true, script: 'git rev-parse HEAD~1').trim()
                        }
                        //Clear existing status checks
                        def jsonBlob = getGithubStatusJsonBlob("pending", "${BUILD_URL}display/redirect", "Full Build In Progress...", "CX Jenkins/Full Build")
                        postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
                        jsonBlob = getGithubStatusJsonBlob("pending", "${BUILD_URL}display/redirect", "OWASP In Progress...", "CX Jenkins/OWASP")
                        postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
                        jsonBlob = getGithubStatusJsonBlob("pending", "${BUILD_URL}display/redirect", "NSP In Progress...", "CX Jenkins/NSP")
                        postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
                    }
                }
            }
        }
        stage('Full Build') {
            //Uncomment this if enabling incremental builds
            //when { expression { env.CHANGE_ID == null } }
            steps {
                timeout(time: 4, unit: 'HOURS') {
                    withMaven(maven: 'M35', globalMavenSettingsConfig: 'default-global-settings', mavenSettingsConfig: 'codice-maven-settings', mavenOpts: '${LARGE_MVN_OPTS} ${LINUX_MVN_RANDOM}', options: [artifactsPublisher(disabled: true)]) {
                        sh '''
                            unset JAVA_TOOL_OPTIONS
                            mvn clean install -DskipITs -e $DISABLE_DOWNLOAD_PROGRESS_OPTS -DkeepRuntimeFolder=true
                        '''
                    }
                }
            }
            //On a failure, archive artifacts that may be useful in debugging the failure
            post {
                success {
                    withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                        script {
                            def jsonBlob = getGithubStatusJsonBlob("success", "${BUILD_URL}display/redirect", "Full Build Succeeded!", "CX Jenkins/Full Build")
                            postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
                        }
                    }
                }
                failure {
                    catchError{ junit '**/target/surefire-reports/*.xml' }
                    catchError{ junit '**/target/failsafe-reports/*.xml' }
                    catchError{ zip zipFile: 'PaxExamRuntimeFolder.zip', archive: true, glob: '**/target/exam/**/*' }
                    withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                        script {
                            def jsonBlob = getGithubStatusJsonBlob("failure", "${BUILD_URL}display/redirect", "Full Build Failed!", "CX Jenkins/Full Build")
                            postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
                        }
                    }
                }
            }
        }
//        stage('Security Analysis') {
//            parallel {
//                stage('owasp') {
//                    steps{
//                        withMaven(maven: 'M35', jdk: 'jdk8-latest', globalMavenSettingsConfig: 'gsr_maven_global_settings', mavenOpts: '${LARGE_MVN_OPTS} ${LINUX_MVN_RANDOM}', options: [artifactsPublisher(disabled: true)]) {
//                            sh 'mvn package -e -q -Powasp -DskipTests=true -DskipStatic=true $DISABLE_DOWNLOAD_PROGRESS_OPTS'
//                        }
//                    }
//                    post {
//                        success {
//                            script {
//                                withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
//                                    def jsonBlob = getGithubStatusJsonBlob("success", "${BUILD_URL}display/redirect", "OWASP Succeeded!", "CX Jenkins/OWASP")
//                                    postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
//                                }
//                            }
//                        }
//                        failure {
//                            //TODO: This zip approach still isn't quite working. Need to figure out why since whenever it's run and fails locally, the files are there
//                            //Might be related to PRs using gib to build PRs, so switching to a non incremental OWASP scan might help
//                            catchError{ zip zipFile: 'OWASP_Reports.zip', archive: true, glob: "target/OWASP_Reports/*" }
//                            script {
//                                withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
//                                    def jsonBlob = getGithubStatusJsonBlob("failure", "${BUILD_URL}display/redirect", "OWASP Failed!", "CX Jenkins/OWASP")
//                                    postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
//                                }
//                            }
//                        }
//                    }
//                }
//                stage('nodeJsSecurity') {
//                    agent { label 'linux-small' }
//                    steps {
//                        script {
//                            def packageFiles = findFiles(glob: '**/package.json')
//                            for (int i = 0; i < packageFiles.size(); i++) {
//                                dir(packageFiles[i].path.split('package.json')[0]) {
//                                    def packageFile = readJSON file: 'package.json'
//                                    if (packageFile.scripts =~ /.*webpack.*/ || packageFile.containsKey("browserify")) {
//                                        nodejs(configId: 'npmrc-default', nodeJSInstallationName: 'nodejs') {
//                                            echo "Scanning ${packageFiles[i].path}"
//                                            sh 'nsp check'
//                                        }
//                                    }
//                                }
//                            }
//                        }
//                    }
//                    post {
//                        success {
//                            script {
//                                withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
//                                    def jsonBlob = getGithubStatusJsonBlob("success", "${BUILD_URL}display/redirect", "NSP Succeeded!", "CX Jenkins/NSP")
//                                    postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
//                                }
//                            }
//                        }
//                        failure {
//                            script {
//                                withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
//                                    def jsonBlob = getGithubStatusJsonBlob("failure", "${BUILD_URL}display/redirect", "NSP Failed!", "CX Jenkins/NSP")
//                                    postStatusToHash("${jsonBlob}", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${env.PR_COMMIT}", "${GITHUB_TOKEN}")
//                                }
//                            }
//                        }
//                    }
//                }
//            }
//        }
        stage('Deploy') {
            when {
                allOf {
                    expression { env.CHANGE_ID == null }
                    expression { env.BRANCH_NAME ==~ /\d*\.x/ }
                    environment name: 'JENKINS_ENV', value: 'prod'
                }
            }
            steps{
                withMaven(maven: 'M3', jdk: 'jdk8-latest', globalMavenSettingsConfig: 'default-global-settings', mavenSettingsConfig: 'codice-maven-settings', mavenOpts: '${LINUX_MVN_RANDOM}') {
                    sh 'mvn deploy -B -DskipStatic=true -DskipTests=true -DretryFailedDeploymentCount=10 -nsu $DISABLE_DOWNLOAD_PROGRESS_OPTS -P \\!docker'
                }
            }
        }
    }
    post {
        success {
            slackSend color: 'good', message: "SUCCESS: ${JOB_NAME} ${BUILD_NUMBER}"
            withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                postCommentIfPR("Build success! See the job results in [legacy Jenkins UI](${BUILD_URL}) or in [Blue Ocean UI](${BUILD_URL}display/redirect).", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${GITHUB_TOKEN}")
            }
        }
        failure {
            slackSend color: '#ea0017', message: "FAILURE: ${JOB_NAME} ${BUILD_NUMBER}. See the results here: ${BUILD_URL}"
            withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                postCommentIfPR("Build failure. See the job results in [legacy Jenkins UI](${BUILD_URL}) or in [Blue Ocean UI](${BUILD_URL}display/redirect).", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${GITHUB_TOKEN}")
            }
        }
        unstable {
            slackSend color: '#ffb600', message: "UNSTABLE: ${JOB_NAME} ${BUILD_NUMBER}. See the results here: ${BUILD_URL}"
            withCredentials([usernameColonPassword(credentialsId: 'cxbot', variable: 'GITHUB_TOKEN')]) {
                postCommentIfPR("Build unstable. See the job results in [legacy Jenkins UI](${BUILD_URL}) or in [Blue Ocean UI](${BUILD_URL}display/redirect).", "${GITHUB_USERNAME}", "${GITHUB_REPONAME}", "${GITHUB_TOKEN}")
            }
        }
        cleanup {
            echo '...Cleaning up workspace'
            cleanWs()
            sh 'rm -rf ~/.m2/repository'
            wrap([$class: 'MesosSingleUseSlave']) {
                sh 'echo "...Shutting down single-use slave: `hostname`"'
            }
        }
    }
}
