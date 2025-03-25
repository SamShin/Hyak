# .bashrc

# Source global definitions
if [ -f /etc/bashrc ]; then
	. /etc/bashrc
fi

# User specific environment
if ! [[ "$PATH" =~ "$HOME/.local/bin:$HOME/bin:" ]]
then
    PATH="$HOME/.local/bin:$HOME/bin:$PATH"
fi
export PATH

# Uncomment the following line if you don't like systemctl's auto-paging feature:
# export SYSTEMD_PAGER=

# User specific aliases and functions

alias apptainer_run='apptainer run --env PS1="[\u@Apptainer \W]\$ "'
alias apptainer_shell='apptainer shell --env PS1="[\u@Apptainer \W]\$ "'
alias apptainer_start='apptainer shell --env PS1="[Apptainer@ \W]\$" --bind /mmfs1/home/seunguk/spark/work:/spark/spark-3.4.0-bin-hadoop3/work,/mmfs1/home/seunguk/spark/conf:/spark/spark-3.4.0-bin-hadoop3/conf /mmfs1/home/seunguk/apptainer/def_sif_files/python.sif'
#alias python3='/bin/python3.9'

# Linux commands
alias rm='rm -I --preserve-root'
alias ..='cd ..'
alias mv='mv -i'
alias cp='cp -i'
alias ln='ln -i'
alias ls='ls --color'

# System info 
alias meminfo='free -h'
alias cpuinfo='lscpu'

# Tmux
alias tm_ls='tmux list-sessions'
alias tm_conn='tmux attach -t'
alias tm_rm='tmux kill-session -t'
alias tm_rename='rename-session'
alias c='clear'

# Hyak
alias halloc='hyakalloc'
alias hstorage='hyakstorage'

# Test
alias gpu_alloc='salloc --partition=ckpt-g2 --reservation=NodeTesting --gpus=1'
alias gpu_alloc_large='salloc --partition=ckpt-g2 --reservation=NodeTesting --gpus=8'
export PATH="$PATH:/mmfs1/home/seunguk/.local/bin"

# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/gscratch/stf/seunguk/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/gscratch/stf/seunguk/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/gscratch/stf/seunguk/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/gscratch/stf/seunguk/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<

