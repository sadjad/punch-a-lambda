temp_inbound_message = ""
messages = []
receive_state = 0
expected_length = 4

def process(stuff):
    global temp_inbound_message, receive_state, expected_length, messages
    temp_inbound_message += stuff
    print(temp_inbound_message)
    if receive_state == 0:
        if len(temp_inbound_message) > 4:
            expected_length = int(temp_inbound_message[:4])
            if len(temp_inbound_message) > expected_length - 1:
                messages.append(temp_inbound_message[4:expected_length])
                temp_inbound_message = temp_inbound_message[expected_length:]
                receive_state = 0
                expected_length = 4
            else:
                receive_state = 1
        else:
            receive_state = 0
    else:
        if len(temp_inbound_message) > expected_length - 1:
            messages.append(temp_inbound_message[4:expected_length])
            temp_inbound_message = temp_inbound_message[expected_length:]
            expected_length = 4
            receive_state = 0
        else:
            receive_state = 1


process("00")
process("05b")
process("0007ba")
process("c")
print(messages)
