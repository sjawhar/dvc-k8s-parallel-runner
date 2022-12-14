#!/usr/bin/env groovy
properties([
    parameters([
        string(
            name: 'REPO_NAME',
            description: 'Name of repo to run. Format: {project_name}/{repo_name}',
            trim: true,
        ),
        string(
            name: 'BUILD_BRANCH',
            description: 'Branch to build',
            trim: true,
        ),
        string(
            name: 'PIPELINES',
            description: 'Space-separated list of pipeline names to run. Leave blank to run all',
            trim: true,
        ),
        string(
            name: 'TARGET_BRANCH',
            trim: true,
            defaultValue: 'master',
        ),
        string(
            name: 'WORKER_SIZE',
            trim: true,
            description: 'Worker size in CC-MMMM format (number of CPU cores and memory in MB)',
            defaultValue: '4-10240',
        ),
        string(
            name: 'NUM_WORKERS',
            trim: true,
            description: 'Number of workers to spawn',
            defaultValue: '10',
        ),
        choice(
            name: 'USE_GPU',
            choices: ['none', 'map', 'reduce', 'all'],
            description: 'Which parts of the job need a GPU',
        ),
        string(
            name: 'PR_LINK',
            trim: true,
            defaultValue: '',
        ),
        string(
            name: 'DVC_REMOTE_NAME',
            description: 'Name of the DVC remote for the repo (defaults to repo name, without project name)',
            trim: true,
            defaultValue: '',
        ),
    ]),
])

def repoName = params.REPO_NAME.toLowerCase()

def gitSshCredentialId = 'git-ssh-credentials'
def gitUserPassCredentialId = 'bitbucket-notifier-credentials'
def gitDomain = env.BITBUCKET_SSH_URL - ~/ssh:\/\/git@/ - ~/:\d+/

def slackChannel = '#metrics-runner'
def slackWebhookUrlCredentialId = 'slack-webhook-url'

def devGroupId = '123456'
def cacheMountPath = '/dvc_cache'
def cacheServerPath = '/scratch/dvc_caches'

def repoParts = repoName.split('/') as List
def dvcRemoteName = params.DVC_REMOTE_NAME ?: repoParts[1]
def buildName = (['runner', env.BUILD_NUMBER]).join('-').toLowerCase()
def dockerImageName = "${repoParts[1]}:runner-${env.BUILD_NUMBER}"

def gitCheckout = { ->
    stage('Git Checkout') {
        checkout    changelog: true,
                    poll: false,
                    scm: [
                        $class: 'GitSCM',
                        branches: [[name: params.BUILD_BRANCH]],
                        browser: [
                            $class: 'BitbucketWeb',
                            repoUrl: "${env.BITBUCKET_HTTP_URL}/projects/${repoParts[0]}/repos/${repoParts[1]}",
                        ],
                        extensions: [
                            [$class: 'CleanBeforeCheckout', deleteUntrackedNestedRepositories: true],
                            [$class: 'LocalBranch'],
                        ],
                        userRemoteConfigs: [[
                            credentialsId: gitSshCredentialId,
                            url: "${env.BITBUCKET_SSH_URL}/${repoParts[0]}/${repoParts[1]}.git",
                        ]]
                    ]

        sshagent(credentials: [gitSshCredentialId]) {
            sh """#!/bin/bash
                set -ex

                mkdir -p ~/.ssh
                ssh-keyscan -p 7999 ${gitDomain} >> ~/.ssh/known_hosts

                git config --global --add safe.directory \$(pwd)
                git config --global user.name "Jenkins User"
                git config --global user.email "jenkins@acme.corp"

                git fetch origin ${params.TARGET_BRANCH}
            """
        }
    }
}

def configureDvc = { ->
    stage('Configure DVC') {
        sh """#!/bin/bash
            set -eufx -o pipefail

            dvc config --local cache.dir ${cacheMountPath}/${repoName}/dvc
            dvc config --local cache.shared group
            dvc config --local cache.type reflink,hardlink,symlink

            dvc remote modify --local ${dvcRemoteName} url ${cacheMountPath}/${repoName}/dvc

            id
        """
    }
}

def gitCommit = { commitArgs, pushArgs ->
    sh """#!/bin/bash
        set -ex

        git add .
        git commit ${commitArgs}
    """

    sshagent(credentials: [gitSshCredentialId]) {
        sh "git push ${pushArgs} --set-upstream origin ${params.BUILD_BRANCH}"
    }
}

def isChangedInGit = { ->
    1 == sh(script: 'git diff --quiet --exit-code', returnStatus: true)
}

def sendSlack = { ->
    withCredentials([
        string(credentialsId: slackWebhookUrlCredentialId, variable: 'SLACK_WEBHOOK_URL')
    ]) {
        sh """
            curl -X POST \
                --data '{
                    "channel": "${slackChannel}",
                    "text": "Runner failed for ${params.REPO_NAME}: ${BUILD_URL}"
                }' \
                \${SLACK_WEBHOOK_URL}
        """
    }
}

def pipelines = params.PIPELINES.trim()
node('docker') {
    try {
        gitCheckout()

        notifyBitbucket buildStatus: 'INPROGRESS',
                        buildName: buildName

        if (!pipelines) {
            pipelines = sh(
                script: 'find . -type f -name dvc.yaml',
                returnStdout: true,
            ).trim().split('\n').join(' ')
        } else {
            pipelines = pipelines.split(' ').collect { "${it}/dvc.yaml" }.join(' ')
        }

        stage('Build Docker Image') {
            def image = null
            withEnv(['DOCKER_BUILDKIT=1']) {
                sshagent(credentials: [gitSshCredentialId]) {
                    image = docker.build(
                        dockerImageName,
                        [
                            "--build-arg DVC_DEVICE=${params.USE_GPU == 'none' ? 'cpu' : 'gpu'}",
                            "--build-arg GID=${devGroupId}",
                            '--target=prod',
                            '--ssh default=$SSH_AUTH_SOCK',
                            '.'
                        ].join(' ')
                    )
                }
            }

            image.push()
        }
    } catch (error) {
        sendSlack()
        throw (error)
    }
}

def useGpu = ['all', 'map'].contains(params.USE_GPU)
def nodeSelector = "nodetype=${useGpu ? 'gpu' : 'cpu'}"
def isCommited = false
podTemplate(
    label: buildName,
    nodeSelector: nodeSelector,
    containers: [
       containerTemplate(
            name: buildName,
            image: 'neuromancer:latest',
            alwaysPullImage: true,
            command: '/bin/bash',
            ttyEnabled: true,
            runAsGroup: devGroupId,
            resourceRequestCpu: '2',
            resourceLimitCpu: '2',
            resourceRequestMemory: '1G',
            resourceLimitMemory: '4G',
        ),
    ],
    volumes: [
        nfsVolume(
            mountPath: cacheMountPath,
            serverAddress: '10.0.20.3',
            serverPath: cacheServerPath,
            readOnly: false
        ),
    ],
) {
    node(buildName) {
        container(buildName) {
            try {
                if (useGpu) {
                    throw new Exception("GPU for map stage of runner is not yet implemented")
                }

                gitCheckout()
                configureDvc()

                stage('Pull DVC Cache') {
                    sshagent(credentials: [gitSshCredentialId]) {
                        sh([
                            "dvc pull --with-deps ${pipelines}",
                            'echo "WARNING: failed to pull all DVC cache, repro might fail"',
                        ].join(" || "))
                    }
                }

                stage('Launch Workers') {
                    sh([
                        'wintermute',
                        '--verbose',
                        '--provider github.com/iterative/iterative',
                        "--machine ${params.WORKER_SIZE}",
                        "--disk-size 20",
                        "--workers ${params.NUM_WORKERS}",
                        '--timeout 48',
                        '--retries 3',
                        '--no-fetch',
                        "--nfs 10.0.42.1 ${cacheServerPath} ${cacheMountPath}",
                        "--node-selector ${nodeSelector}",
                        '--storage-class portworx-sc',
                        '--tag app.kubernetes.io/instance=jenkins',
                        dockerImageName,
                        params.PIPELINES,
                    ].join(" ").trim())
                }

                stage('Commit and Push') {
                    if (!isChangedInGit()) {
                        echo 'No changes to commit'
                        return
                    }

                    gitCommit('-m "Parallel repro (Step 1 of 2)"', '')
                    isCommited = true
                }
            } catch (error) {
                sendSlack()
                throw (error)
            } finally {
                notifyBitbucket buildStatus: 'SUCCESSFUL',
                                buildName: buildName
            }
        }
    }
}

useGpu = ['all', 'reduce'].contains(params.USE_GPU)
podTemplate(
    label: buildName,
    nodeSelector: "nodetype=${useGpu ? 'gpu' : 'cpu'}",
    containers: [
        containerTemplate(
            name: buildName,
            image: dockerImageName,
            alwaysPullImage: true,
            command: '/bin/bash',
            ttyEnabled: true,
            resourceRequestCpu: '4',
            resourceLimitCpu: '8',
            resourceRequestMemory: '8G',
            resourceLimitMemory: '64G',
        ),
    ],
    volumes: [
        nfsVolume(
            mountPath: cacheMountPath,
            serverAddress: '10.0.20.3',
            serverPath: cacheServerPath,
            readOnly: false
        ),
    ],
) {
    podTemplate(
        yaml: """
            spec:
              containers:
                - name: ${buildName}
                  resources:
                    limits:
                      nvidia.com/gpu: '${useGpu ? 1 : 0}'
                    requests:
                      nvidia.com/gpu: '${useGpu ? 1 : 0}'
        """
    ) {
        node(POD_LABEL) {
            container(buildName) {
                try {
                    gitCheckout()
                    configureDvc()

                    def diffReportFile = 'diff_report.md'
                    stage('Finish Pipelines') {
                        sshagent(credentials: [gitSshCredentialId]) {
                            sh([
                                "dvc pull --with-deps ${pipelines}",
                                "dvc checkout --with-deps ${pipelines}",
                                'echo "WARNING: failed to pull all DVC cache, repro might fail"',
                            ].join(" || "))
                        }

                        withEnv([
                            'MLFLOW_TRACKING_URI=https://mlflow.acme.corp',
                        ]) {
                            sh "dvc repro ${pipelines}"
                        }

                        withCredentials([
                            aws(
                                credentialsId: 'jenkins-minio-credentials',
                                accessKeyVariable: 'AWS_ACCESS_KEY_ID',
                                secretKeyVariable: 'AWS_SECRET_ACCESS_KEY',
                            ),
                        ]) {
                            withEnv([
                                'AWS_ENDPOINT_URL=https://minio.acme.corp',
                            ]) {

                                sh """#!/bin/bash
                                    set -ex

                                    if ! [ -x "\$(command -v diff_report)" ]
                                    then
                                        diff_report() {
                                            echo 'Repro complete (no diff_report in repo)'
                                        }
                                    fi
                                    diff_report --precision 5 origin/${params.TARGET_BRANCH} | tee ${diffReportFile}
                                """
                            }
                        }
                    }

                    stage('Commit and Push') {
                        if (!isCommited && !isChangedInGit()) {
                            return
                        }
                        def commitArgs = '--message="Auto-repro"'
                        def pushArgs = ''
                        if (isCommited) {
                            commitArgs += ' --amend'
                            pushArgs += ' --force'
                        }
                        gitCommit(commitArgs, pushArgs)
                    }

                    stage('Diff Report') {
                        if (!params.PR_LINK) {
                            return
                        }

                        withCredentials([
                            usernamePassword(
                                credentialsId: gitUserPassCredentialId,
                                passwordVariable: 'BITBUCKET_PASSWORD',
                                usernameVariable: 'BITBUCKET_USERNAME',
                            )
                        ]) {
                            sh """#!/bin/bash
                                set -ex

                                jq --rawfile diff_report ${diffReportFile} -cnr '{"text": \$diff_report}' | tee comment.json

                                curl \
                                    --cacert ${caFile} \
                                    --data @comment.json \
                                    --fail \
                                    --header 'Content-Type: application/json' \
                                    --request POST \
                                    --show-error \
                                    --user "\${BITBUCKET_USERNAME}:\${BITBUCKET_PASSWORD}" \
                                    "\$(echo ${params.PR_LINK}/comments | sed 's|/projects/|/rest/api/1.0/projects/|')"
                            """
                        }
                    }
                } catch (error) {
                    sendSlack()
                    throw (error)
                } finally {
                    notifyBitbucket buildStatus: 'SUCCESSFUL',
                                    buildName: buildName
                }
            }
        }
    }
}
