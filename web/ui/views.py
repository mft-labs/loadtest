from django.shortcuts import render
import glob
from ui import loadtest2
import time
from ui.models import Tests
# Create your views here.

def home(request):
    config_files = glob.glob('../*.json')
    print(config_files)
    return render(request,'index.html',{'conf_list':config_files})

def run_test(request):
    print('Arrived to run_test')
    conf = request.GET.get('conf','')
    result = loadtest2.run_manager(conf)
    time.sleep(5)
    new_entity = Tests(run_time=result, log_file = f'loadtest_{result}.log',
                        db_file= f'loadtest_{result}.db')
    new_entity.save()
    details = open(f'loadtest_{result}.log','r').read()
    return render(request,'result.html',{'timestamp':result,'details':details})

def get_log_files(request):
    log_files = glob.glob('*.log')
    return render(request,'logs.html',{'log_list':log_files})


def show_log(request):
    log_file = request.GET.get('logfile','')
    details = open(f'{log_file}','r').read()
    return render(request,'show-log.html',{'details':details})

def show_details(request):
    tests = Tests.objects.all()
    return render(request,'tests_list.html',{'entries':tests})
