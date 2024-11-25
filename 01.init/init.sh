# Crear el usuario hadoop y asignarle una contraseña
sudo useradd -m -s /bin/bash hadoop
echo "hadoop:HMauricio159!!!" | sudo chpasswd

# Dar privilegios sin necesidad de escribir sudo
echo "hadoop ALL=(ALL) NOPASSWD:ALL" | sudo tee /etc/sudoers.d/hadoop

# Cambiar al perfil del usuario hadoop
sudo su - hadoop <<EOF
# Aquí van los comandos que deseas ejecutar como hadoop
echo "Soy el usuario hadoop"
# Puedes agregar más comandos aquí
whoami
EOF
