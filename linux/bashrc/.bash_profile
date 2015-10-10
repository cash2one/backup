# .bash_profile

# Get the aliases and functions
if [ -f ~/.bashrc ]; then
	. ~/.bashrc
fi

# User specific environment and startup programs
export MAC=64
export LANG=zh_CN.gbk
NM="\[\033[0m\]" #means no background and white lines
HI="\[\033[0;37m\]" #change this for letter colors
HII="\[\033[0;31m\]" #change this for letter colors
SI="\[\033[0;33m\]" #this is for the current directory
IN="\[\033[0m\]"
export PS1="$NM[ $HI\u@$HII\H:$SI\w$NM ] $IN"
export mail_account=luoruiyang

PATH=$PATH:$HOME/bin

export PATH
unset USERNAME

alias python="/root/.jumbo/bin/python"
alias home="cd /home/users/luoruiyang/"
alias hadoop="cd /home/users/luoruiyang/tasks/hadoop/"
alias ranker="cd /home/users/code/app/ecom/im/ranker && svn up"
alias prod="cd /home/users/code/app/ecom/im/production && svn up"
alias cr="/home/users/luoruiyang/tools/autocr/autocr -i"
alias deploy="/root/.jumbo/bin/python /home/users/luoruiyang/tools/auto-deploy/deploy.py"
alias go="cd /home/users/code/app/ecom/im/production/prod/platform_descriptor/ && cd ${1}"


PATH=/root/.BCloud/bin:$PATH
export PATH
PATH=/root/localbuild_other_tools/scmtools/usr/bin:$PATH
export PATH
