import os
import shutil
import time
import subprocess
import argparse

# Retorna o numero de espacos livres na fila do SLURM
def check_free_spot(user):

    temp = 'check_free_spot.txt'
    script = f'''squeue -u {user} > {temp}'''

    count_lines = subprocess.Popen(script, stdout=subprocess.PIPE, shell=True)
    time.sleep(2)
    with open(temp, 'r') as file:
        lines = file.readlines()

    n_lines = 21-len(lines)

    os.remove(temp)

    return n_lines

# Submete o comando do script
def submit_job(job_path):

    job = job_path.split('/')[-1]
    shell_command='sbatch '+job
    job_path=job_path.replace(job,'')

    submit = subprocess.Popen(shell_command,
                              stdout=subprocess.PIPE,
                              shell=True,
                              cwd=job_path)
    status = submit.communicate()[0].decode('UTF-8')

    if status.split()[0] == 'Submitted':
        print('Submission successful.')
        response=True
    else:
        print('Submission failed.')
        response=False

    return(response)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-jb', '--jobs', default = 'jobs_submetidos.txt',
                        help = 'Nome do arquivo de jobs submetidos.', type = str)

    parser.add_argument('-fjb', '--fjobs', default = 'jobs_afazer',
                        help = 'Pasta com arquivos de jobs a serem submetidos.', type = str)

    parser.add_argument('-u', '--user', default = 'usuario_sd',
                        help = 'Usuario do SLURM.', type = str)

    args = parser.parse_args()
    # Verifica se existe um arquivo com os jobs ja nfeitos
    if not(os.path.isfile(args.jobs)):
        f= open(args.jobs,"w+")
        f.close()
        nfeitos=0
    else:
        with open(args.jobs, 'r') as file:
            jobs_prontos = file.readlines()
        nfeitos=len(jobs_prontos)


    sleep_time = 1800
    qts_jobs = len(os.listdir(args.fjobs))
    todo_jobs = qts_jobs-nfeitos # qtd de jobs que ainda precisam ser feitos
    i = 0


    while(i<todo_jobs):

        with open(args.jobs, 'r') as file:
            jobs_prontos = file.readlines()

        jobs_prontos=[job.replace('\n','') for job in jobs_prontos] # So tira o \n

        for folder in os.listdir(args.fjobs):
            # O job ainda nao foi submetido
            if not(folder in jobs_prontos):
                n_lines = check_free_spot(args.user)
                # Tem espaco para colocar na fila
                if n_lines>0:
                    job_folder = args.fjobs+'/'+folder
                    job_file = [file for file in os.listdir(job_folder) if 'job' in file][0]
                    submit_job(job_folder+'/'+job_file)
                    with open(args.jobs, 'a') as file:
                        file.write(folder+'\n')
                    i = i+1
                else:
                    break
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
