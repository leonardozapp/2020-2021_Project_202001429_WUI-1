<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * LogsAtividades Controller
 *
 * @property \App\Model\Table\LogsAtividadesTable $LogsAtividades
 * @method \App\Model\Entity\LogsAtividade[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class LogsAtividadesController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $logsAtividades = $this->paginate($this->LogsAtividades);

        $this->set(compact('logsAtividades'));
    }

    /**
     * View method
     *
     * @param string|null $id Logs Atividade id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $logsAtividade = $this->LogsAtividades->get($id, [
            'contain' => [],
        ]);

        $this->set(compact('logsAtividade'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $logsAtividade = $this->LogsAtividades->newEmptyEntity();
        if ($this->request->is('post')) {
            $logsAtividade = $this->LogsAtividades->patchEntity($logsAtividade, $this->request->getData());
            if ($this->LogsAtividades->save($logsAtividade)) {
                $this->Flash->success(__('The logs atividade has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The logs atividade could not be saved. Please, try again.'));
        }
        $this->set(compact('logsAtividade'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Logs Atividade id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $logsAtividade = $this->LogsAtividades->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $logsAtividade = $this->LogsAtividades->patchEntity($logsAtividade, $this->request->getData());
            if ($this->LogsAtividades->save($logsAtividade)) {
                $this->Flash->success(__('The logs atividade has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The logs atividade could not be saved. Please, try again.'));
        }
        $this->set(compact('logsAtividade'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Logs Atividade id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $logsAtividade = $this->LogsAtividades->get($id);
        if ($this->LogsAtividades->delete($logsAtividade)) {
            $this->Flash->success(__('The logs atividade has been deleted.'));
        } else {
            $this->Flash->error(__('The logs atividade could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
