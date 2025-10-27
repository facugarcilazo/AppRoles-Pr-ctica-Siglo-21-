import argparse
import sys
import os
import oracledb
from typing import List, Dict
from dotenv import load_dotenv

class RoleManager:
    def __init__(self, db_connection):
        """Inicializa el manager de roles con una conexion a la DB y carga roles"""
        self.connection = db_connection
        self.roles = self.load_roles_from_db()
        self.roles_by_name = {name: id for id, name in self.roles.items()}

    def load_roles_from_db(self) -> Dict[int, str]:
        """Carga los roles desde la DB"""
        if not self.connection:
            raise ValueError("Una conexion con la base de datos es requerida.")

        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT id, nombre FROM ue21.ue_roles")
            roles = {row[0]: row[1] for row in cursor.fetchall()}
            return roles
        except oracledb.Error as e:
            print(f"Error cargando roles: {e}", file=sys.stderr)
            return {}
        finally:
            cursor.close()

    def role_exists(self, role_id: int) -> bool:
        """Revisa si un rol id existe en la DB"""
        return role_id in self.roles

    def get_user_id(self, user: str):
        """Traer ID de usuario de la DB."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT ID FROM ue21.ue_usuario WHERE USR_PORTAL = :usr", usr=user)
            result = cursor.fetchone()
            return result[0] if result else None
        except oracledb.Error as e:
            print(f"Error trayendo usuario '{user}': {e}", file=sys.stderr)
            return None
        finally:
            cursor.close()

    def list_all_roles(self):
        """Muestra todos los roles disponibles."""
        if not self.roles:
            print("No se encontraron roles en la DB", file=sys.stderr)
            return
        
        print("Roles disponibles:")
        print("=" * 50)
        print(f"{'ID':<5} | {'Nombre de Rol'}")
        print("-" * 50)
        
        for role_id, role_name in sorted(self.roles.items()):
            print(f"{role_id:<5} | {role_name}")
        
        print(f"\nTotal de roles: {len(self.roles)}")

    def check_user_role(self, user: str, role_id: int) -> bool:
        """Revisa si un usuario tiene un rol en especifico"""
        if not self.connection:
            raise ValueError("Una conexion con la base de datos es requerida")

        cursor = self.connection.cursor()
        try:
            user_id = self.get_user_id(user)
            if not user_id:
                print(f"Warning: Usuario '{user}' no existente", file=sys.stderr)
                return False

            if not self.role_exists(role_id):
                print(f"Error: ID de Rol {role_id} no existente", file=sys.stderr)
                return False

            cursor.execute(
                "SELECT 1 FROM ue21.ue_usuario_roles WHERE rol_id = :rol_id AND usr_id = :usr_id",
                rol_id=role_id, usr_id=user_id
            )
            has_role = cursor.fetchone() is not None
            
            role_name = self.roles.get(role_id, '')
            if has_role:
                print(f"✓ Usuario '{user}' tiene el rol {role_id} ({role_name})")
            else:
                print(f"✗ Usuario '{user}' no tiene el rol {role_id} ({role_name})")
            
            return has_role

        except oracledb.Error as e:
            print(f"Error de Oracle al revisar el rol: {e}", file=sys.stderr)
            return False

        finally:
            cursor.close()

    def grant_role(self, user: str, role_id: int) -> bool:
        """Dar rol a un usuario."""
        if not self.connection:
            raise ValueError("Una conexion con la base de datos es requerida.")

        cursor = self.connection.cursor()
        try:
            user_id = self.get_user_id(user)
            if not user_id:
                print(f"Warning: Usuario '{user}' no existente", file=sys.stderr)
                return False

            if not self.role_exists(role_id):
                print(f"Error: ID de Rol {role_id} no existente", file=sys.stderr)
                return False

            cursor.execute(
                "SELECT 1 FROM ue21.ue_usuario_roles WHERE rol_id = :rol_id AND usr_id = :usr_id",
                rol_id=role_id, usr_id=user_id
            )
            if cursor.fetchone():
                print(f"Info: Usuario '{user}' ya tiene el rol {role_id} ({self.roles.get(role_id, '')})")
                return True

            # Get the next available ID for new role assignment
            cursor.execute("SELECT MAX(ID) + 1 FROM ue21.UE_USUARIO_ROLES")
            max_id_result = cursor.fetchone()
            new_id = max_id_result[0] if max_id_result[0] is not None else 1  # If no records, start from 1

            cursor.execute(
                "INSERT INTO ue21.ue_usuario_roles (id, rol_id, usr_id) VALUES (:id, :rol_id, :usr_id)",
                id=new_id, rol_id=role_id, usr_id=user_id
            )
            self.connection.commit()
            print(f"Success: Se concedio el rol {role_id} ({self.roles.get(role_id, '')}) al usuario '{user}'")
            return True

        except oracledb.IntegrityError as e:
            if 'ORA-02291' in str(e):
                print(f"Error: ID de Rol {role_id} no existente (violacion de FK)", file=sys.stderr)
            else:
                print(f"Error de integridad de Oracle: {e}", file=sys.stderr)
            self.connection.rollback()
            return False

        except oracledb.Error as e:
            print(f"Error de Oracle: {e}", file=sys.stderr)
            self.connection.rollback()
            return False

        finally:
            cursor.close()


def load_db_config():
    """Carga configuracion desde variables de entorno."""
    # Load .env file
    load_dotenv()
    
    # Get required environment variables
    config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'port': int(os.getenv('DB_PORT', '1521')),  # Default to 1521 if not specified
        'sid': os.getenv('DB_SID'),
        'service': os.getenv('DB_SERVICE')
    }
    
    # Validate required parameters
    required_params = ['user', 'password', 'host', 'sid']
    missing_params = [param for param in required_params if not config[param]]
    
    if missing_params:
        print(f"Error: Faltan vairables de entorno: {', '.join([f'DB_{param.upper()}' for param in missing_params])}", file=sys.stderr)
        print("Por favor revisar que el .env tenga todas las variables requeridas. Referir a la documentacion.", file=sys.stderr)
        sys.exit(1)
    
    return config


def connect_to_db(config: dict) -> oracledb.Connection:
    """Establece una conexion con OracleDB."""
    try:
        oracledb.init_oracle_client()
        
        dsn = oracledb.makedsn(config['host'], config['port'], config['sid'])
        return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)
    except oracledb.Error as e:
        print(f"Error de conexion con la DB: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Manejo de roles de usuarios en base de datos Oracle.")
    parser.add_argument("--action", required=True, choices=["check", "grant", "list-roles"], help="Accion a realizar")
    parser.add_argument("--user_name", help="Comma-separated list of users to check or grant (No requerido para list-roles)")
    parser.add_argument("--roles", help="Comma-separated list of role IDs to check or grant (No requerido para list-roles)")

    args = parser.parse_args()

    # Load database configuration from .env
    db_config = load_db_config()
    
    # Connect to database
    connection = connect_to_db(db_config)
    role_manager = RoleManager(db_connection=connection)

    try:
        if args.action == "list-roles":
            # Just list all roles, no need for users or roles arguments
            role_manager.list_all_roles()
        else:
            # Validate required arguments for other actions
            if not args.user_name or not args.roles:
                print("Error: --user_name y --roles son requeridos para acciones check y grant", file=sys.stderr)
                sys.exit(1)
            
            roles = [int(role_id.strip()) for role_id in args.roles.split(',')]
            users = [user.strip() for user in args.user_name.split(',')]

            if args.action == "grant":
                for user in users:
                    for role_id in roles:
                        role_manager.grant_role(user, role_id)
            elif args.action == "check":
                for user in users:
                    for role_id in roles:
                        role_manager.check_user_role(user, role_id)

    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()