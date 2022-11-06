# chmod 755 init.sh (if necessary)

# download fork of egui to use as a local dependency
project_dir=$(pwd)
egui_link="$project_dir/crates/egui"
if [ ! -d "$egui_link" ]; then
    cd ..
    git clone https://github.com/yay/egui.git
    cd egui
    egui_dir=$(pwd)
    git remote add upstream https://github.com/emilk/egui.git
    mkdir $project_dir/crates
    ln -s $egui_dir $egui_link
fi