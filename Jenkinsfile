pipeline {
    agent any

    environment {
        DOCKERHUB_REPO = "kovidms/pms-transactional"
        IMAGE_TAG = "latest"
    }

    stages {
        stage('Git Checkout'){
            steps {
                checkout([$class: 'GitSCM',
                    branches: [[name: 'main']],
                    extensions: [[$class: 'RelativeTargetDirectory', relativeTargetDir: '.']],
                    userRemoteConfigs: [[url: 'https://github.com/pms-org/pms-transactional.git']]
                ])
            }
        }

        stage('DEBUG CREDENTIALS') {
            steps {
                echo "Checking credentials..."
                withCredentials([file(credentialsId: 'pms-env-file', variable: 'ENV_FILE')]) {
                    echo "ENV file credential loaded successfully!"
                }
            }
        }

        stage('Debug Workspace') {
            steps {
                sh 'pwd'
                sh 'ls -R .'
            }
        }

       stage('Build Docker Image') {
            steps {
                sh """
                docker build --progress=plain --no-cache -t ${DOCKERHUB_REPO}:${IMAGE_TAG} .

                """
            }
        }

        stage('Login & Push to DockerHub') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'dockerhub-creds',
                    usernameVariable: 'DOCKER_USER',
                    passwordVariable: 'DOCKER_PASS'
                )]) {
                    sh """
                    echo "$DOCKER_PASS" | docker login -u "$DOCKER_USER" --password-stdin
                    docker push ${DOCKERHUB_REPO}:${IMAGE_TAG}
                    """
                }
            }
        }
        
    }

    post {
        success { echo "Docker Upload successful" }
        failure { echo "Docker Upload Failed" }
    }

}