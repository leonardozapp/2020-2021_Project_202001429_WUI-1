<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * Permissoes Controller
 *
 * @property \App\Model\Table\PermissoesTable $Permissoes
 * @method \App\Model\Entity\Permisso[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class PermissoesController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $permissoes = $this->paginate($this->Permissoes);

        $this->set(compact('permissoes'));
    }

    /**
     * View method
     *
     * @param string|null $id Permisso id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $permisso = $this->Permissoes->get($id, [
            'contain' => ['TipoPlanos'],
        ]);

        $this->set(compact('permisso'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $permisso = $this->Permissoes->newEmptyEntity();
        if ($this->request->is('post')) {
            $permisso = $this->Permissoes->patchEntity($permisso, $this->request->getData());
            if ($this->Permissoes->save($permisso)) {
                $this->Flash->success(__('The permisso has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The permisso could not be saved. Please, try again.'));
        }
        $this->set(compact('permisso'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Permisso id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $permisso = $this->Permissoes->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $permisso = $this->Permissoes->patchEntity($permisso, $this->request->getData());
            if ($this->Permissoes->save($permisso)) {
                $this->Flash->success(__('The permisso has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The permisso could not be saved. Please, try again.'));
        }
        $this->set(compact('permisso'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Permisso id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $permisso = $this->Permissoes->get($id);
        if ($this->Permissoes->delete($permisso)) {
            $this->Flash->success(__('The permisso has been deleted.'));
        } else {
            $this->Flash->error(__('The permisso could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
