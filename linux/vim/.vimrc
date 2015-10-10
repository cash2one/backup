set noswapfile
au BufRead,BufNewFile *.scala set filetype=scala
au! Syntax scala source ~/.vim/syntax/scala.vim

set t_Co=256
set fdm=marker
nnoremap ; :
:command W w
:command P set paste
:command WQ wq
:command Wq wq
:command Q qu
:command Qa qa
:command QA qa
:command Wqall wqall
:command WQall wqall
:command V vertical all

set fileencodings=utf-8,cp936,gb18030,big5,euc-jp,sjis,euc-kr,ucs-2le,latin1
set nocompatible
filetype on            " enables filetype detections
filetype plugin on     " enables filetype specific plugins
set iskeyword+=-,_
set guifont=Menlo\ Regular:h14
set mouse=
set wrap
set sidescroll=1
set sidescrolloff=15
set so=5
set cursorcolumn
set cursorline 
set wildmenu
set ruler 
set tabstop=4
set shiftwidth=4
set softtabstop=4
set smartindent
set ai
set expandtab 
set number
set hlsearch 
set backspace=indent,eol,start
set showcmd 
set textwidth=0 
set autoindent
syntax enable 
colorscheme molokai

