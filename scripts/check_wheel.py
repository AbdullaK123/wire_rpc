"""Install the built wheel in an isolated environment and smoke-test exports."""
import argparse
import sysconfig
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import venv
import zipfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--reuse-dependencies', action='store_true',
                        help='Use the current locked dependency environment for an offline artifact check')
    args = parser.parse_args()
    wheels = list((Path(__file__).resolve().parents[1] / 'dist').glob('*.whl'))
    if len(wheels) != 1:
        raise RuntimeError('Expected exactly one wheel in dist/')
    wheel = wheels[0]
    with zipfile.ZipFile(wheel) as archive:
        if 'wire_rpc/py.typed' not in archive.namelist():
            raise RuntimeError('Wheel is missing its typing marker')
    with tempfile.TemporaryDirectory() as directory:
        environment = Path(directory) / 'env'
        venv.EnvBuilder().create(environment)
        python = environment / ('Scripts/python.exe' if os.name == 'nt' else 'bin/python')
        if not args.reuse_dependencies:
            requirements = Path(directory) / 'requirements.txt'
            subprocess.run(['uv','export','--locked','--no-dev','--no-emit-project',
                            '--format','requirements-txt','--output-file',str(requirements)],
                           cwd=Path(__file__).resolve().parents[1],check=True,stdout=subprocess.DEVNULL)
            subprocess.run(['uv','pip','install','--python',str(python),'-r',str(requirements)],check=True)
        subprocess.run(['uv','pip','install','--python',str(python),str(wheel),'--no-deps'],check=True)
        prefix = ('import sys; sys.path.append(' + repr(sysconfig.get_paths()['purelib']) + '); ') if args.reuse_dependencies else ''
        env = dict(os.environ)
        env.pop('PYTHONPATH',None)
        subprocess.run([str(python),'-I','-c',
            prefix + 'import wire_rpc; from pathlib import Path; import sys; '
            'assert Path(wire_rpc.__file__).is_relative_to(Path(sys.prefix)), '
            '"smoke test must import the installed wheel, never the checkout"; '
            'from wire_rpc import App, MulticastApp, Client, current_request; '
            'from wire_rpc.transports.tcp import TcpClientTransport; '
            'from wire_rpc.codecs.msgspec import MsgSpecMsgPackCodec; '
            'assert MsgSpecMsgPackCodec().convert({"x":1}, dict) == {"x":1}, '
            '"installed wheel must retain codec conversion behavior"'],cwd=directory,env=env,check=True)
    print('Installed wheel imports and codec smoke check passed')


if __name__ == '__main__':
    main()
