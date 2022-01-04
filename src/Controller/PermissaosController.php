<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * Permissaos Controller
 *
 * @property \App\Model\Table\PermissaosTable $Permissaos
 * @method \App\Model\Entity\Permissao[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class PermissaosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $permissaos = $this->paginate($this->Permissaos);

        $this->set(compact('permissaos'));
    }

    /**
     * View method
     *
     * @param string|null $id Permissao id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $permissao = $this->Permissaos->get($id, [
            'contain' => ['TipoPlanos'],
        ]);

        $this->set(compact('permissao'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $permissao = $this->Permissaos->newEmptyEntity();
        if ($this->request->is('post')) {
            $permissao = $this->Permissaos->patchEntity($permissao, $this->request->getData());
            if ($this->Permissaos->save($permissao)) {
                $this->Flash->success(__('The permissao has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The permissao could not be saved. Please, try again.'));
        }
        $this->set(compact('permissao'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Permissao id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $permissao = $this->Permissaos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $permissao = $this->Permissaos->patchEntity($permissao, $this->request->getData());
            if ($this->Permissaos->save($permissao)) {
                $this->Flash->success(__('The permissao has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The permissao could not be saved. Please, try again.'));
        }
        $this->set(compact('permissao'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Permissao id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $permissao = $this->Permissaos->get($id);
        if ($this->Permissaos->delete($permissao)) {
            $this->Flash->success(__('The permissao has been deleted.'));
        } else {
            $this->Flash->error(__('The permissao could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
