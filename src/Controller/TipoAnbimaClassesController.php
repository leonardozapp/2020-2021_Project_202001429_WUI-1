<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoAnbimaClasses Controller
 *
 * @property \App\Model\Table\TipoAnbimaClassesTable $TipoAnbimaClasses
 * @method \App\Model\Entity\TipoAnbimaClass[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoAnbimaClassesController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoAnbimaClasses = $this->paginate($this->TipoAnbimaClasses);

        $this->set(compact('tipoAnbimaClasses'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Anbima Class id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoAnbimaClass = $this->TipoAnbimaClasses->get($id, [
            'contain' => [],
        ]);

        $this->set(compact('tipoAnbimaClass'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoAnbimaClass = $this->TipoAnbimaClasses->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoAnbimaClass = $this->TipoAnbimaClasses->patchEntity($tipoAnbimaClass, $this->request->getData());
            if ($this->TipoAnbimaClasses->save($tipoAnbimaClass)) {
                $this->Flash->success(__('The tipo anbima class has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo anbima class could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoAnbimaClass'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Anbima Class id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoAnbimaClass = $this->TipoAnbimaClasses->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoAnbimaClass = $this->TipoAnbimaClasses->patchEntity($tipoAnbimaClass, $this->request->getData());
            if ($this->TipoAnbimaClasses->save($tipoAnbimaClass)) {
                $this->Flash->success(__('The tipo anbima class has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo anbima class could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoAnbimaClass'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Anbima Class id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoAnbimaClass = $this->TipoAnbimaClasses->get($id);
        if ($this->TipoAnbimaClasses->delete($tipoAnbimaClass)) {
            $this->Flash->success(__('The tipo anbima class has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo anbima class could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
