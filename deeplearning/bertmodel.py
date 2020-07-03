#!/usr/bin/python
# -*- coding:utf-8 -*- 
'''
作者:jia.zhou@aliyun.com
创建时间:2020-06-30 下午2:23
'''
import os

from tensorflow import keras

import bert

model_dir = "/home/eos/chinese_L-12_H-768_A-12"

bert_params = bert.params_from_pretrained_ckpt(model_dir)
l_bert = bert.BertModelLayer.from_params(bert_params, name="bert")


max_seq_len = 128
l_input_ids      = keras.layers.Input(shape=(max_seq_len,), dtype='int32')
l_token_type_ids = keras.layers.Input(shape=(max_seq_len,), dtype='int32')

# using the default token_type/segment id 0
output = l_bert(l_input_ids)                              # output: [batch_size, max_seq_len, hidden_size]
model = keras.Model(inputs=l_input_ids, outputs=output)
model.build(input_shape=(None, max_seq_len))

# provide a custom token_type/segment id as a layer input
# output = l_bert([l_input_ids, l_token_type_ids])          # [batch_size, max_seq_len, hidden_size]
# model = keras.Model(inputs=[l_input_ids, l_token_type_ids], outputs=output)
# model.build(input_shape=[(None, max_seq_len), (None, max_seq_len)])

l_bert.apply_adapter_freeze()

bert_ckpt_file   = os.path.join(model_dir, "bert_model.ckpt")
bert.load_stock_weights(l_bert, bert_ckpt_file)


vocab_file = os.path.join(model_dir, "vocab.txt")
tokenizer = bert.albert_tokenization.FullTokenizer(vocab_file=vocab_file)
tokens = tokenizer.tokenize(u"你好世界")
token_ids = tokenizer.convert_tokens_to_ids(tokens)