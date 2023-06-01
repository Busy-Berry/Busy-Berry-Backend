import torch
print(torch.cuda.is_available())
torch.device('cuda')
print(torch.__version__)
with torch.device('cuda:0'):
    r = torch.randn(2, 3)
print(r.device)