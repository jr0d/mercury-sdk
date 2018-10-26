# TODO: modify cmd2 to support this use case
# import cmd2
import colorama

from mercury_sdk.rpc.job import SimpleJob


PROMPT = f'{colorama.Style.BRIGHT}{colorama.Fore.LIGHTBLUE_EX}(♀)︎{colorama.Fore.MAGENTA}~>' \
         f'{colorama.Style.RESET_ALL} '

class MercuryShell:
    """ Ridiculously simple shell (no readline support) """
    def __init__(self, rpc_client, prompt=PROMPT, initial_query=None):
        self.rpc_client = rpc_client
        self.prompt = prompt
        self.query = initial_query

    def input_loop(self):
        while True:
            try:
                instruction = input(self.prompt)
            except KeyboardInterrupt:
                print()
                continue
            except EOFError:
                print()
                break

            if not instruction:
                continue

            if instruction == 'exit':
                break

            if instruction.strip()[0] == '!':
                if not len(instruction) > 1:
                    print('Shell command missing')
                print('THIS IS A SHELL ESCAPE: {}'.format(instruction[1:]))
                continue

            s = SimpleJob(self.rpc_client, self.query, 'run', job_args=[instruction])
            s.start()
            s.join(poll_interval=.2)

            for t in s.tasks['tasks']:
                stdout = t['message']['stdout']
                if stdout:
                    print(stdout)
                stderr = t['message']['stderr']
                if stderr:
                    print(stderr)


if __name__ == '__main__':
    from mercury_sdk.http.rpc import JobInterfaceBase
    ib = JobInterfaceBase('http://localhost:9005')
    s = MercuryShell(ib, initial_query={})
    s.input_loop()
